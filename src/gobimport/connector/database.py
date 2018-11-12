"""Implementation of a database input connectors

The following connectors are implemented in this module:
    Database - Connects to a (oracle) database using connection details

"""
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import DBAPIError

from gobcore.exceptions import GOBException

from gobimport.connector.config import DATABASE_CONFIGS


def connect_to_database(source):
    """Connect to the datasource

    The SQLAlchemy library is used to connect to the data source for databases

    :return: a connection to the given database
    """
    # Get the database config based on the source name
    try:
        DB = DATABASE_CONFIGS[source['name']]
    except KeyError:
        raise GOBException(f"Database config for source {source['name']} not found.")

    user = f"({DB['username']}@{DB['database']})"
    try:
        engine = create_engine(URL(**DB))
        connection = engine.connect()

    except DBAPIError as e:
        raise GOBException(f'Database connection {user} failed. Error: {e}.')
    else:
        return connection, user
