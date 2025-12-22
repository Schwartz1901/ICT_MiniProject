"""
ML Pipeline - Machine Learning & Anomaly Detection

Performs ML-related tasks including:
- Training data preparation
- Price prediction using Random Forest
- Anomaly detection
- Model results storage
"""

import logging
import json
from datetime import datetime
from snowflake_client.config import get_connection, execute_query, close_connection
from sql import load_query

logger = logging.getLogger(__name__)

# Model version for tracking
MODEL_VERSION = "v1.0.0"
MODEL_TYPE = "RandomForestRegressor"


def create_ml_tables():
    """Create tables for storing ML model results."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(load_query("ml", "create_price_predictions_table"))
    logger.info("Created/verified: ANALYTICS_DB.ML_RESULTS.PRICE_PREDICTIONS")

    cursor.execute(load_query("ml", "create_anomaly_alerts_table"))
    logger.info("Created/verified: ANALYTICS_DB.ML_RESULTS.ANOMALY_ALERTS")

    cursor.execute(load_query("ml", "create_model_registry_table"))
    logger.info("Created/verified: ANALYTICS_DB.ML_RESULTS.MODEL_REGISTRY")

    cursor.execute(load_query("ml", "create_feature_importance_table"))
    logger.info("Created/verified: ANALYTICS_DB.ML_RESULTS.FEATURE_IMPORTANCE")

    cursor.close()


def get_training_data():
    """Fetch ML training data from Gold layer."""
    query = load_query("ml", "get_training_data")
    data = execute_query(query)
    logger.info(f"Retrieved {len(data)} records for ML training")
    return data


def get_training_data_stats():
    """Get statistics about training data."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(load_query("ml", "get_training_data_stats"))
    stats = cursor.fetchone()
    cursor.close()

    if stats[0] and stats[0] > 0:
        logger.info("Training Data Statistics:")
        logger.info(f"  - Total records: {stats[0]}")
        logger.info(f"  - Unique cities: {stats[1]}")
        if stats[2] and stats[3]:
            logger.info(f"  - Avg price: {stats[2]:,.0f} (std: {stats[3]:,.0f})")
        if stats[4] and stats[5]:
            logger.info(f"  - Avg area: {stats[4]:,.1f} (std: {stats[5]:,.1f})")
        if stats[6]:
            logger.info(f"  - Avg price/sqm: {stats[6]:,.0f}")
        logger.info(f"  - Categories - Budget: {stats[7]}, Standard: {stats[8]}, Premium: {stats[9]}, Luxury: {stats[10]}")

    return stats


def train_price_prediction_model():
    """
    Train a Random Forest model for house price prediction.

    Returns:
        dict: Model metrics (r2, mae, rmse)
    """
    try:
        import pandas as pd
        import numpy as np
        from sklearn.model_selection import train_test_split
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.preprocessing import LabelEncoder
        from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
    except ImportError as e:
        logger.warning(f"ML libraries not installed: {e}. Skipping price prediction.")
        return None

    logger.info("Training price prediction model...")

    # Fetch training data
    data = get_training_data()
    if not data or len(data) < 100:
        logger.warning("Insufficient training data for price prediction")
        return None

    # Convert to DataFrame
    columns = ['id', 'price', 'price_per_sqm', 'area', 'bedrooms', 'bathrooms',
               'rooms_total', 'city', 'district', 'latitude', 'longitude',
               'price_category', 'area_category', 'is_high_value']
    df = pd.DataFrame(data, columns=columns)

    # Prepare features
    feature_columns = ['area', 'bedrooms', 'bathrooms', 'rooms_total', 'latitude', 'longitude']

    # Encode categorical features
    le_city = LabelEncoder()
    le_district = LabelEncoder()

    df['city_encoded'] = le_city.fit_transform(df['city'].fillna('Unknown').astype(str))
    df['district_encoded'] = le_district.fit_transform(df['district'].fillna('Unknown').astype(str))

    feature_columns.extend(['city_encoded', 'district_encoded'])

    # Prepare X and y
    X = df[feature_columns].fillna(0)
    y = df['price']

    # Filter valid data
    valid_mask = (y > 0) & (X['area'] > 0)
    X = X[valid_mask]
    y = y[valid_mask]
    ids = df.loc[valid_mask, 'id'].values

    if len(X) < 100:
        logger.warning("Insufficient valid training data")
        return None

    # Train/test split
    X_train, X_test, y_train, y_test, ids_train, ids_test = train_test_split(
        X, y, ids, test_size=0.2, random_state=42
    )

    logger.info(f"Training set: {len(X_train)}, Test set: {len(X_test)}")

    # Train Random Forest model
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=15,
        min_samples_split=5,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train, y_train)

    # Make predictions
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)

    # Calculate metrics
    train_r2 = r2_score(y_train, y_pred_train)
    test_r2 = r2_score(y_test, y_pred_test)
    test_mae = mean_absolute_error(y_test, y_pred_test)
    test_rmse = np.sqrt(mean_squared_error(y_test, y_pred_test))

    logger.info(f"Model Performance:")
    logger.info(f"  - Train R²: {train_r2:.4f}")
    logger.info(f"  - Test R²: {test_r2:.4f}")
    logger.info(f"  - Test MAE: {test_mae:,.0f} VND")
    logger.info(f"  - Test RMSE: {test_rmse:,.0f} VND")

    # Store predictions
    store_predictions(ids_test, y_test.values, y_pred_test, feature_columns)

    # Store feature importance
    store_feature_importance(feature_columns, model.feature_importances_)

    # Register model
    metrics = {
        'train_r2': float(train_r2),
        'test_r2': float(test_r2),
        'test_mae': float(test_mae),
        'test_rmse': float(test_rmse),
        'train_samples': int(len(X_train)),
        'test_samples': int(len(X_test))
    }
    register_model(metrics)

    return metrics


def store_predictions(ids, actual_prices, predicted_prices, features_used):
    """Store price predictions in Snowflake."""
    conn = get_connection()
    cursor = conn.cursor()

    # Delete existing predictions for this model version
    cursor.execute(f"""
        DELETE FROM ANALYTICS_DB.ML_RESULTS.PRICE_PREDICTIONS
        WHERE model_version = '{MODEL_VERSION}'
    """)

    # Insert predictions
    insert_sql = """
        INSERT INTO ANALYTICS_DB.ML_RESULTS.PRICE_PREDICTIONS
        (id, predicted_price, actual_price, prediction_error, prediction_error_pct,
         model_version, model_type, features_used)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    batch = []
    for i, (id_, actual, predicted) in enumerate(zip(ids, actual_prices, predicted_prices)):
        error = predicted - actual
        error_pct = (error / actual * 100) if actual != 0 else 0
        features_json = json.dumps(features_used)
        batch.append((id_, float(predicted), float(actual), float(error),
                      float(error_pct), MODEL_VERSION, MODEL_TYPE, features_json))

    cursor.executemany(insert_sql, batch)
    logger.info(f"Stored {len(batch)} price predictions")
    cursor.close()


def store_feature_importance(feature_names, importances):
    """Store feature importance scores."""
    conn = get_connection()
    cursor = conn.cursor()

    # Delete existing for this model version
    cursor.execute(f"""
        DELETE FROM ANALYTICS_DB.ML_RESULTS.FEATURE_IMPORTANCE
        WHERE model_version = '{MODEL_VERSION}'
    """)

    # Insert feature importance
    insert_sql = """
        INSERT INTO ANALYTICS_DB.ML_RESULTS.FEATURE_IMPORTANCE
        (model_version, model_type, feature_name, importance_score, rank)
        VALUES (%s, %s, %s, %s, %s)
    """

    # Sort by importance
    sorted_features = sorted(zip(feature_names, importances), key=lambda x: x[1], reverse=True)

    batch = []
    for rank, (name, importance) in enumerate(sorted_features, 1):
        batch.append((MODEL_VERSION, MODEL_TYPE, name, float(importance), rank))

    cursor.executemany(insert_sql, batch)

    logger.info("Feature Importance:")
    for name, importance in sorted_features[:5]:
        logger.info(f"  - {name}: {importance:.4f}")

    cursor.close()


def register_model(metrics):
    """Register trained model in the model registry."""
    conn = get_connection()
    cursor = conn.cursor()

    # Check if version exists
    cursor.execute(f"""
        SELECT COUNT(*) FROM ANALYTICS_DB.ML_RESULTS.MODEL_REGISTRY
        WHERE model_version = '{MODEL_VERSION}'
    """)
    exists = cursor.fetchone()[0] > 0

    metrics_json = json.dumps(metrics)

    if exists:
        cursor.execute("""
            UPDATE ANALYTICS_DB.ML_RESULTS.MODEL_REGISTRY
            SET metrics = %s,
                is_active = TRUE,
                updated_at = CURRENT_TIMESTAMP()
            WHERE model_version = %s
        """, (metrics_json, MODEL_VERSION))
    else:
        cursor.execute("""
            INSERT INTO ANALYTICS_DB.ML_RESULTS.MODEL_REGISTRY
            (model_version, model_type, model_name, metrics, is_active)
            VALUES (%s, %s, %s, %s, TRUE)
        """, (MODEL_VERSION, MODEL_TYPE, 'House Price Predictor', metrics_json))

    logger.info(f"Registered model {MODEL_VERSION} in registry")
    cursor.close()


def detect_price_anomalies():
    """Detect price anomalies using statistical methods."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(load_query("ml", "detect_price_anomalies"))
    anomaly_count = cursor.rowcount
    logger.info(f"Detected {anomaly_count} new price anomalies")
    cursor.close()
    return anomaly_count


def detect_price_per_sqm_anomalies():
    """Detect properties with unusual price per sqm."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(load_query("ml", "detect_price_per_sqm_anomalies"))
    anomaly_count = cursor.rowcount
    logger.info(f"Detected {anomaly_count} new price/sqm anomalies")
    cursor.close()
    return anomaly_count


def get_anomaly_summary():
    """Get summary of detected anomalies."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(load_query("ml", "get_anomaly_summary"))
    results = cursor.fetchall()
    cursor.close()

    if results:
        logger.info("Anomaly Summary (Unresolved):")
        for row in results:
            logger.info(f"  - {row[0]} [{row[1]}]: {row[2]} alerts (avg score: {row[3]:.2f})")
    else:
        logger.info("No unresolved anomalies found")

    return results


def run():
    """Run ML pipeline step."""
    try:
        logger.info("Starting ML Pipeline...")

        # Setup tables
        create_ml_tables()

        # Get training data stats
        get_training_data_stats()

        # Train price prediction model
        logger.info("Training price prediction model...")
        metrics = train_price_prediction_model()
        if metrics:
            logger.info(f"Price prediction model trained successfully (R²: {metrics['test_r2']:.4f})")

        # Run anomaly detection
        logger.info("Running anomaly detection...")
        detect_price_anomalies()
        detect_price_per_sqm_anomalies()

        # Summary
        get_anomaly_summary()

        logger.info("ML Pipeline completed successfully")
    except Exception as e:
        logger.error(f"Error in ML Pipeline: {e}", exc_info=True)
        raise
    finally:
        close_connection()
