"""
Snowflake Setup - Database and Schema Configuration

Sets up the Medallion architecture in Snowflake:
- Databases: RAW_DB, STAGING_DB, ANALYTICS_DB
- Schemas: LANDING, CLEANED, DWH, ML_RESULTS, SEMANTIC
- Tables and Views for each layer
"""

import logging
from snowflake_client.config import get_connection, close_connection
from sql import load_query

logger = logging.getLogger(__name__)


def setup_databases():
    """Create databases for Medallion architecture."""
    conn = get_connection()
    cursor = conn.cursor()

    databases = [
        ("RAW_DB", "Bronze layer - raw ingested data"),
        ("STAGING_DB", "Silver layer - cleaned and transformed data"),
        ("ANALYTICS_DB", "Gold layer - analytics and ML ready data"),
    ]

    for db_name, description in databases:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} COMMENT = '{description}'")
        logger.info(f"Created/verified database: {db_name}")

    cursor.close()


def setup_schemas():
    """Create schemas within each database."""
    conn = get_connection()
    cursor = conn.cursor()

    schemas = [
        # Bronze layer schemas
        ("RAW_DB", "LANDING", "Raw data landing zone from Kafka"),
        # Silver layer schemas
        ("STAGING_DB", "CLEANED", "Cleaned and validated data"),
        # Gold layer schemas
        ("ANALYTICS_DB", "DWH", "Data warehouse - analytics ready"),
        ("ANALYTICS_DB", "ML_RESULTS", "Machine learning results and predictions"),
        ("ANALYTICS_DB", "SEMANTIC", "Semantic layer for BI tools"),
    ]

    for db_name, schema_name, description in schemas:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {db_name}.{schema_name} COMMENT = '{description}'")
        logger.info(f"Created/verified schema: {db_name}.{schema_name}")

    cursor.close()


def setup_bronze_tables():
    """Create Bronze layer tables."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("USE DATABASE RAW_DB")
    cursor.execute("USE SCHEMA LANDING")
    cursor.execute(load_query("setup", "create_bronze_table"))
    logger.info("Created/verified: RAW_DB.LANDING.BRONZE_REAL_ESTATE")

    cursor.close()


def setup_silver_tables():
    """Create Silver layer tables."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("USE DATABASE STAGING_DB")
    cursor.execute("USE SCHEMA CLEANED")
    cursor.execute(load_query("setup", "create_silver_table"))
    logger.info("Created/verified: STAGING_DB.CLEANED.SILVER_REAL_ESTATE")

    cursor.close()


def setup_gold_tables():
    """Create Gold layer tables."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("USE DATABASE ANALYTICS_DB")
    cursor.execute("USE SCHEMA DWH")

    cursor.execute(load_query("setup", "create_gold_ml_features"))
    logger.info("Created/verified: ANALYTICS_DB.DWH.GOLD_ML_FEATURES")

    cursor.execute(load_query("setup", "create_gold_market_analytics"))
    logger.info("Created/verified: ANALYTICS_DB.DWH.GOLD_MARKET_ANALYTICS")

    cursor.execute(load_query("setup", "create_gold_price_trends"))
    logger.info("Created/verified: ANALYTICS_DB.DWH.GOLD_PRICE_TRENDS")

    cursor.close()


def setup_ml_tables():
    """Create ML results tables."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("USE DATABASE ANALYTICS_DB")
    cursor.execute("USE SCHEMA ML_RESULTS")

    cursor.execute(load_query("setup", "create_ml_price_predictions"))
    logger.info("Created/verified: ANALYTICS_DB.ML_RESULTS.PRICE_PREDICTIONS")

    cursor.execute(load_query("setup", "create_ml_anomaly_alerts"))
    logger.info("Created/verified: ANALYTICS_DB.ML_RESULTS.ANOMALY_ALERTS")

    cursor.execute(load_query("setup", "create_ml_model_registry"))
    logger.info("Created/verified: ANALYTICS_DB.ML_RESULTS.MODEL_REGISTRY")

    cursor.execute(load_query("setup", "create_ml_feature_importance"))
    logger.info("Created/verified: ANALYTICS_DB.ML_RESULTS.FEATURE_IMPORTANCE")

    cursor.close()


def setup_semantic_views():
    """Create semantic layer views for BI tools."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("USE DATABASE ANALYTICS_DB")
    cursor.execute("USE SCHEMA SEMANTIC")

    cursor.execute(load_query("setup", "create_view_property_listings"))
    logger.info("Created/verified: ANALYTICS_DB.SEMANTIC.V_PROPERTY_LISTINGS")

    cursor.execute(load_query("setup", "create_view_market_summary"))
    logger.info("Created/verified: ANALYTICS_DB.SEMANTIC.V_MARKET_SUMMARY")

    cursor.execute(load_query("setup", "create_view_price_trends"))
    logger.info("Created/verified: ANALYTICS_DB.SEMANTIC.V_PRICE_TRENDS")

    cursor.execute(load_query("setup", "create_view_active_anomalies"))
    logger.info("Created/verified: ANALYTICS_DB.SEMANTIC.V_ACTIVE_ANOMALIES")

    cursor.close()


def verify_setup():
    """Verify all objects were created successfully."""
    conn = get_connection()
    cursor = conn.cursor()

    # Check databases
    cursor.execute("SHOW DATABASES LIKE 'RAW_DB'")
    raw_db = cursor.fetchone()

    cursor.execute("SHOW DATABASES LIKE 'STAGING_DB'")
    staging_db = cursor.fetchone()

    cursor.execute("SHOW DATABASES LIKE 'ANALYTICS_DB'")
    analytics_db = cursor.fetchone()

    cursor.close()

    if raw_db and staging_db and analytics_db:
        logger.info("Verification: All databases created successfully")
        return True
    else:
        logger.error("Verification failed: Some databases are missing")
        return False


def run_full_setup():
    """Run complete Snowflake setup for Medallion architecture."""
    logger.info("=" * 60)
    logger.info("Starting Snowflake Medallion Architecture Setup")
    logger.info("=" * 60)

    try:
        logger.info("\n[1/7] Setting up databases...")
        setup_databases()

        logger.info("\n[2/7] Setting up schemas...")
        setup_schemas()

        logger.info("\n[3/7] Setting up Bronze layer tables...")
        setup_bronze_tables()

        logger.info("\n[4/7] Setting up Silver layer tables...")
        setup_silver_tables()

        logger.info("\n[5/7] Setting up Gold layer tables...")
        setup_gold_tables()

        logger.info("\n[6/7] Setting up ML results tables...")
        setup_ml_tables()

        logger.info("\n[7/7] Setting up Semantic layer views...")
        setup_semantic_views()

        logger.info("\n" + "=" * 60)
        if verify_setup():
            logger.info("Snowflake setup completed successfully!")
        else:
            logger.error("Snowflake setup completed with errors")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error during setup: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    try:
        run_full_setup()
    except Exception as e:
        logger.error(f"Setup failed: {e}")
    finally:
        close_connection()
        logger.info("Connections closed")
