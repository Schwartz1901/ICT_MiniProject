import os
import certifi
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())

# Get the default database from the connection string (or specify one)
db = client.get_database(os.getenv("MONGO_DATABASE", "nhandata"))


def get_client():
    """Return the MongoDB client instance."""
    return client


def get_database(name: str = None):
    """Return a database instance.

    Args:
        name: Database name. If None, returns the default database.
    """
    if name:
        return client.get_database(name)
    return db


def get_collection(collection_name: str, database_name: str = None):
    """Return a collection instance.

    Args:
        collection_name: Name of the collection.
        database_name: Database name. If None, uses the default database.
    """
    database = get_database(database_name)
    return database.get_collection(collection_name)


def close_connection():
    """Close the MongoDB connection."""
    client.close()
