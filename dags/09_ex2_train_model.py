from __future__ import annotations
import pendulum

import os

import pandas as pd
import polars as pl

import joblib

from airflow import DAG
from airflow.decorators import task
from airflow.providers.standard.operators.python import get_current_context

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from sklearn.linear_model import ElasticNet
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import PoissonRegressor

from sklearn.model_selection import GridSearchCV

from sklearn.metrics import mean_absolute_error


BUCKET = "taxi-data"
AWS_CONN_ID = "aws_default"
PG_CONN_ID = "db_fx"


with DAG(
    dag_id="taxi_training_model",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@monthly",
    catchup=False,
    tags=["ml", "scikit-learn", "taxi"],
) as dag:

    @task
    def fetch_data() -> str:
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        keys = hook.list_keys(bucket_name=BUCKET, prefix="processed/daily/")

        local_files = []

        for k in keys:
            local_path = f"/tmp/{os.path.basename(k)}"
            hook.get_key(k, bucket_name=BUCKET).download_file(local_path) 
            local_files.append(local_path)

        data = [pl.read_parquet(p) for p in local_files]
        df_full = pl.concat(data).sort("date")

        df_full_path = "/tmp/full_data.parquet"
        df_full.write_parquet(df_full_path)

        return df_full_path

    @task
    def split_train_test(path: str) -> dict:
        df = pl.read_parquet(path)
        df = df.with_columns(
            pl.col("date").cast(pl.Date),
            pl.col("date").dt.strftime("%Y-%m").alias("ym").cast(pl.Utf8)
        )

        last_month = df.select(pl.col("ym").max()).item()

        train = df.filter(pl.col("ym") != last_month)
        test = df.filter(pl.col("ym") == last_month)

        train_path = "/tmp/train.parquet"
        test_path = "/tmp/test.parquet"
        train.write_parquet(train_path)
        test.write_parquet(test_path)

        return {
            "train_path": train_path,
            "test_path": test_path,
            "test_month": last_month,
            "train_size": train.height,
            "test_size": test.height,
        }

    def make_features(df: pd.DataFrame):
        X = df[["date"]].copy()
        X["day_of_week"] = X["date"].dt.dayofweek
        X["day_of_month"] = X["date"].dt.day
        X["month"] = X["date"].dt.month

        X = X.drop(columns=["date"])
        y = df["total_rides"]
        return X, y

    @task
    def train_elastic_net(paths: dict) -> dict:
        train_df = pl.read_parquet(paths["train_path"]).to_pandas()
        test_df = pl.read_parquet(paths["test_path"]).to_pandas()

        X_train, y_train = make_features(train_df)
        X_test, y_test = make_features(test_df)

        model = ElasticNet(alpha=1.0, l1_ratio=0.5, random_state=42)
        model.fit(X_train, y_train)

        preds = model.predict(X_test)
        mae = float(mean_absolute_error(y_test, preds))

        local_model_path = f"/tmp/model_elastic_net_{paths['test_month']}.joblib"
        joblib.dump(model, local_model_path)

        return {
            "model_name": "elastic_net",
            "mae": mae,
            "train_size": int(len(train_df)),
            "local_path": local_model_path,
        }
    @task
    def train_histgradient(paths: dict) -> dict:
        train_df = pl.read_parquet(paths["train_path"]).to_pandas()
        test_df = pl.read_parquet(paths["test_path"]).to_pandas()

        X_train, y_train = make_features(train_df)
        X_test, y_test = make_features(test_df)

        model = HistGradientBoostingRegressor(random_state=42)
        model.fit(X_train, y_train)

        preds = model.predict(X_test)
        mae = float(mean_absolute_error(y_test, preds))

        local_model_path = f"/tmp/model_histgrad_{paths['test_month']}.joblib"
        joblib.dump(model, local_model_path)

        return {
            "model_name": "hist_gradient_boosting",
            "mae": mae,
            "train_size": int(len(train_df)),
            "local_path": local_model_path,
        }
    
    @task
    def train_poisson_tuned(paths: dict) -> dict:
        train_df = pl.read_parquet(paths["train_path"]).to_pandas()
        test_df = pl.read_parquet(paths["test_path"]).to_pandas()

        X_train, y_train = make_features(train_df)
        X_test, y_test = make_features(test_df)

        param_grid = {
            "alpha": [0.01, 0.1, 1.0, 10.0],
            "max_iter": [100, 200, 300],
        }
        base_model = PoissonRegressor()
        grid_search = GridSearchCV(
            estimator=base_model,
            param_grid=param_grid,
            scoring="neg_mean_absolute_error",
            cv=3,
            n_jobs=-1,
        )
        grid_search.fit(X_train, y_train)

        best_model = grid_search.best_estimator_

        preds = best_model.predict(X_test)
        mae = float(mean_absolute_error(y_test, preds))

        local_model_path = f"/tmp/model_poisson_tuned_{paths['test_month']}.joblib"
        joblib.dump(best_model, local_model_path)

        return {
            "model_name": "poisson_regressor_tuned",
            "mae": mae,
            "train_size": int(len(train_df)),
            "local_path": local_model_path,
        }

    @task
    def upload_model(result: dict) -> dict:
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        key = f"models/tmp/{os.path.basename(result['local_path'])}"

        hook.load_file(
            filename=result["local_path"],
            key=key,
            bucket_name=BUCKET,
            replace=True,
        )

        result["s3_uri"] = f"s3://{BUCKET}/{key}"
        return result

    @task
    def select_best(results: list[dict], test_month: str) -> dict:
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        best = min(results, key=lambda r: r["mae"])
        best_key = best["s3_uri"].replace(f"s3://{BUCKET}/", "")
        final_key = f"models/best/best_model_{test_month}.joblib"

        hook.copy_object(
            source_bucket_key=best_key,
            dest_bucket_key=final_key,
            source_bucket_name=BUCKET,
            dest_bucket_name=BUCKET,
        )

        for r in results:
            k = r["s3_uri"].replace(f"s3://{BUCKET}/", "")
            if k != best_key:
                hook.delete_objects(bucket=BUCKET, keys=[k])

        return {
            "best_model": best["model_name"],
            "best_mae": best["mae"],
            "best_s3_path": f"s3://{BUCKET}/{final_key}",
            "results": results,
        }

    @task
    def log_results_to_postgres(summary: dict):
        ctx = get_current_context()
        training_date = ctx["logical_date"]

        rows = []
        for r in summary["results"]:
            rows.append(
                (
                    training_date,
                    r["model_name"],
                    r["train_size"],
                    r["mae"],
                )
            )
        
        pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
        pg.insert_rows(
            table="ml_model_results",
            rows=rows,
            target_fields=["training_date", "model_name", "training_set_size", "test_mae"],
        )



    combined = fetch_data()
    paths = split_train_test(combined)

    elastic_net = upload_model(train_elastic_net(paths))
    hist_gradient = upload_model(train_histgradient(paths))
    poisson_tuned = upload_model(train_poisson_tuned(paths))

    log_results_to_postgres(
        select_best(
            [elastic_net, hist_gradient, poisson_tuned],
            test_month=paths["test_month"],
        )
    )