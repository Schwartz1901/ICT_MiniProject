CREATE TABLE IF NOT EXISTS ANALYTICS_DB.ML_RESULTS.PRICE_PREDICTIONS (
    id VARCHAR,
    predicted_price FLOAT,
    actual_price FLOAT,
    prediction_error FLOAT,
    prediction_error_pct FLOAT,
    model_version VARCHAR,
    model_type VARCHAR,
    features_used VARIANT,
    predicted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (id, model_version)
)
