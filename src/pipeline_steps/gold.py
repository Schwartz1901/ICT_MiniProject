"""
Gold Layer - Analytics & ML Ready Data

Transforms Silver layer data into feature-engineered datasets
ready for ML models and business analytics.
"""

import logging
from snowflake_client.config import get_connection, close_connection
from sql import load_query

logger = logging.getLogger(__name__)

# Minimum data quality score for Gold layer
MIN_QUALITY_SCORE = 0.6


def create_gold_tables():
    """Create Gold layer tables for ML and analytics."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(load_query("gold", "create_ml_features_table"))
    logger.info("Created/verified: ANALYTICS_DB.DWH.GOLD_ML_FEATURES")

    cursor.execute(load_query("gold", "create_market_analytics_table"))
    logger.info("Created/verified: ANALYTICS_DB.DWH.GOLD_MARKET_ANALYTICS")

    cursor.execute(load_query("gold", "create_price_trends_table"))
    logger.info("Created/verified: ANALYTICS_DB.DWH.GOLD_PRICE_TRENDS")

    cursor.close()


def transform_to_ml_features():
    """Transform Silver data to Gold ML features table."""
    conn = get_connection()
    cursor = conn.cursor()

    # Get price percentiles for categorization
    query = load_query("gold", "get_price_percentiles").format(min_quality_score=MIN_QUALITY_SCORE)
    cursor.execute(query)

    percentiles = cursor.fetchone()
    p25 = percentiles[0] if percentiles[0] else 1000000000
    p50 = percentiles[1] if percentiles[1] else 3000000000
    p75 = percentiles[2] if percentiles[2] else 7000000000

    logger.info(f"Price percentiles - P25: {p25:,.0f}, P50: {p50:,.0f}, P75: {p75:,.0f}")

    # Insert/update ML features
    query = load_query("gold", "merge_ml_features").format(
        min_quality_score=MIN_QUALITY_SCORE,
        p25=p25,
        p50=p50,
        p75=p75
    )
    cursor.execute(query)

    features_count = cursor.rowcount
    logger.info(f"Updated {features_count} records in GOLD_ML_FEATURES")

    cursor.close()
    return features_count


def update_market_analytics():
    """Update aggregated market analytics."""
    conn = get_connection()
    cursor = conn.cursor()

    query = load_query("gold", "merge_market_analytics").format(min_quality_score=MIN_QUALITY_SCORE)
    cursor.execute(query)

    analytics_count = cursor.rowcount
    logger.info(f"Updated {analytics_count} market analytics records")

    cursor.close()
    return analytics_count


def update_price_trends():
    """Update price trends time series data."""
    conn = get_connection()
    cursor = conn.cursor()

    query = load_query("gold", "merge_price_trends").format(min_quality_score=MIN_QUALITY_SCORE)
    cursor.execute(query)

    trends_count = cursor.rowcount
    logger.info(f"Updated {trends_count} price trend records")

    cursor.close()
    return trends_count


def get_gold_stats():
    """Get statistics about the Gold layer."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(load_query("gold", "get_stats"))
    stats = cursor.fetchone()
    cursor.close()

    if stats[0] and stats[0] > 0:
        logger.info(f"Gold Layer Stats:")
        logger.info(f"  - Total ML features: {stats[0]}")
        logger.info(f"  - Unique cities: {stats[1]}")
        logger.info(f"  - High value properties: {stats[2]}")
        if stats[3]:
            logger.info(f"  - Avg price: {stats[3]:,.0f}")
        if stats[4]:
            logger.info(f"  - Avg price/sqm: {stats[4]:,.0f}")

    return stats


def get_category_distribution():
    """Get distribution of price and area categories."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(load_query("gold", "get_category_distribution"))
    results = cursor.fetchall()
    cursor.close()

    if results:
        logger.info("Price Category Distribution:")
        for row in results:
            logger.info(f"  - {row[0]}: {row[1]} properties")

    return results


def run():
    """Run Gold layer pipeline step."""
    try:
        logger.info("Starting Silver to Gold transformation...")

        create_gold_tables()
        transform_to_ml_features()
        update_market_analytics()
        update_price_trends()
        get_gold_stats()
        get_category_distribution()

        logger.info("Gold transformation completed successfully")
    except Exception as e:
        logger.error(f"Error in Gold transformation: {e}", exc_info=True)
        raise
    finally:
        close_connection()
