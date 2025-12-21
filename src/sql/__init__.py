"""
SQL Query Loader

Provides utilities to load SQL queries from .sql files.
"""

import os

SQL_DIR = os.path.dirname(os.path.abspath(__file__))


def load_query(category: str, name: str) -> str:
    """
    Load a SQL query from file.

    Args:
        category: Folder name (e.g., 'setup', 'bronze', 'silver', 'gold', 'ml')
        name: SQL file name without extension (e.g., 'create_bronze_table')

    Returns:
        SQL query string
    """
    file_path = os.path.join(SQL_DIR, category, f"{name}.sql")
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()


def load_all_queries(category: str) -> dict:
    """
    Load all SQL queries from a category folder.

    Args:
        category: Folder name (e.g., 'setup', 'bronze')

    Returns:
        Dictionary mapping file names (without extension) to SQL content
    """
    folder_path = os.path.join(SQL_DIR, category)
    queries = {}

    for filename in os.listdir(folder_path):
        if filename.endswith(".sql"):
            name = filename[:-4]  # Remove .sql extension
            queries[name] = load_query(category, name)

    return queries
