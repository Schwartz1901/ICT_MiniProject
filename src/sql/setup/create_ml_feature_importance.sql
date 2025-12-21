CREATE TABLE IF NOT EXISTS FEATURE_IMPORTANCE (
    model_version VARCHAR,
    feature_name VARCHAR,
    importance_score FLOAT,
    rank INT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (model_version, feature_name)
)
COMMENT = 'Feature importance scores from ML models'
