"""
ML Pipeline - Machine Learning & Anomaly Detection

Performs ML-related tasks including:
- Training data preparation
- Anomaly detection
- Model results storage
"""

import logging
from snowflake_client.config import get_connection, execute_query, close_connection
from sql import load_query

logger = logging.getLogger(__name__)


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
