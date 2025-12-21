CREATE TABLE IF NOT EXISTS MODEL_REGISTRY (
    model_id VARCHAR DEFAULT UUID_STRING(),
    model_name VARCHAR,
    model_version VARCHAR,
    model_type VARCHAR,
    training_date TIMESTAMP_NTZ,
    metrics VARIANT,
    parameters VARIANT,
    status VARCHAR DEFAULT 'active',
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (model_id)
)
COMMENT = 'ML model registry and versioning'
