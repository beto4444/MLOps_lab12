# Laboratory 12 homework

Homework is refactoring the code from lab 2 homework, i.e. NYC taxi data preparation.
The goal is to build proper ML pipelines for gathering & preparing the dataset, and training the
predictive model.

Deployment:

1. Use Docker Compose for everything.
2. Airflow should be based on the final lab configuration.
3. Create appropriate S3 bucket(s) and use readable paths for all objects.
4. Properly isolate workers, using virtual environments where necessary.
5. Create an appropriate Postgres table for second DAG (see below).

First DAG should:

1. Download the data in Parquet format and save on S3.
2. Process the data with Polars, save processed dataset on S3.
3. Use backfilling to get the data from last months (depending on your computing power, up to 12).
4. The resulting dataset should have one row per day, with the target being the total number of taxi rides.

Second DAG should:

1. Fetch all data from S3, combine it, use the latest month for testing and previous ones for training.
2. Train a few ML models from scikit-learn as parallel tasks, using the fan-out pattern. Those can be,
   e.g., ridge regression, Random Forest, SVM. Perform some hyperparameter tuning for at least one model.
   Save models on S3 temporarily and pass down the performance (mean absolute error, MAE) of the model.
3. Add a task to select the best model when all finish training, using the fan-in pattern. Save the best
   model to a separate file on S3 and delete other models.
4. Log the performance of all models to an appropriate table in the second Postgres database: training
   date, model name, training set size, test MAE.

You can use either class-based or TaskFlow API. Use appropriate operators, plugins, or Airflow
functionalities as you see fit. For example:

- you can use `BashOperator`, `HttpToS3Operator`, or `PythonOperator` to download the data
- files from S3 can be downloaded with `boto3`, `s3fs`, or directly with Polars integration with `s3fs`
- operations on S3 can use specialized operators

Along code files, include screenshots of:

1. Both DAG graphs
2. DAG executions
3. Lists of files in S3 buckets
4. Postgres table with ML models results
