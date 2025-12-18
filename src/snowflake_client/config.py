import os
import logging
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

logger = logging.getLogger(__name__)

# Snowflake connection configuration
SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
}

_connection = None


def get_connection():
    """Get or create a Snowflake connection."""
    global _connection
    if _connection is None:
        logger.info("Connecting to Snowflake...")
        _connection = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        logger.info("Connected to Snowflake successfully")
    return _connection


def get_cursor():
    """Get a cursor from the Snowflake connection."""
    conn = get_connection()
    return conn.cursor()


def execute_query(query: str, params: tuple = None):
    """Execute a query and return results.

    Args:
        query: SQL query to execute.
        params: Optional query parameters.

    Returns:
        List of rows from the query result.
    """
    cursor = get_cursor()
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor.fetchall()
    finally:
        cursor.close()


def execute_many(query: str, data: list):
    """Execute a query with multiple data rows (batch insert).

    Args:
        query: SQL query to execute.
        data: List of tuples containing the data.

    Returns:
        Number of rows affected.
    """
    cursor = get_cursor()
    try:
        cursor.executemany(query, data)
        return cursor.rowcount
    finally:
        cursor.close()


def close_connection():
    """Close the Snowflake connection."""
    global _connection
    if _connection is not None:
        _connection.close()
        _connection = None
        logger.info("Snowflake connection closed")
