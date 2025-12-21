"""
Silver Layer - Data Cleaning & Transformation

Transforms raw data from Bronze layer into cleaned, structured format
with data quality scoring.
"""

import logging
from snowflake_client.config import get_connection, close_connection
from sql import load_query

logger = logging.getLogger(__name__)


def create_silver_table():
    """Create Silver layer table with cleaned/structured data."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(load_query("silver", "create_table"))
    cursor.close()
    logger.info("Silver table created/verified: STAGING_DB.CLEANED.SILVER_REAL_ESTATE")


def transform_bronze_to_silver():
    """Transform data from Bronze to Silver layer with data cleaning."""
    conn = get_connection()
    cursor = conn.cursor()

    # Get count of new records to process
    cursor.execute(load_query("silver", "count_pending"))
    pending_count = cursor.fetchone()[0]
    logger.info(f"Found {pending_count} new records to transform")

    if pending_count == 0:
        logger.info("No new records to process")
        cursor.close()
        return 0

    # Extract, clean, and transform from Bronze layer
    cursor.execute(load_query("silver", "transform_bronze_to_silver"))
    row_count = cursor.rowcount
    cursor.close()

    logger.info(f"Transformed {row_count} records from Bronze to Silver layer")
    return row_count


def get_silver_stats():
    """Get statistics about the Silver layer data."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(load_query("silver", "get_stats"))
    stats = cursor.fetchone()
    cursor.close()

    if stats[0] and stats[0] > 0:
        logger.info(f"Silver Layer Stats:")
        logger.info(f"  - Total records: {stats[0]}")
        logger.info(f"  - Avg quality score: {stats[1]:.2f}" if stats[1] else "  - Avg quality score: N/A")
        logger.info(f"  - High quality records (>=0.6): {stats[2]}")
        logger.info(f"  - Unique cities: {stats[3]}")
        if stats[4]:
            logger.info(f"  - Avg price: {stats[4]:,.0f}")
        if stats[5]:
            logger.info(f"  - Avg area: {stats[5]:,.1f}")

    return stats


def get_quality_distribution():
    """Get distribution of data quality scores."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(load_query("silver", "get_quality_distribution"))
    results = cursor.fetchall()
    cursor.close()

    if results:
        logger.info("Data Quality Distribution:")
        for row in results:
            logger.info(f"  - {row[0]}: {row[1]} records")

    return results


def run():
    """Run Silver layer pipeline step."""
    try:
        logger.info("Starting Bronze to Silver transformation...")
        create_silver_table()
        rows = transform_bronze_to_silver()
        get_silver_stats()
        get_quality_distribution()
        logger.info("Silver transformation completed successfully")
        return rows
    except Exception as e:
        logger.error(f"Error in Silver transformation: {e}", exc_info=True)
        raise
    finally:
        close_connection()
