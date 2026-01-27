CREATE TABLE IF NOT EXISTS ml_model_results(
    id SERIAL PRIMARY KEY,
    training_date TIMESTAMPTZ NOT NULL,
    model_name TEXT NOT NULL,
    training_set_size INT NOT NULL,
    test_mae DOUBLE PRECISION NOT NULL
);