"""Implementation of a database input connectors

The following connectors are implemented in this module:
    Database - Connects to a (oracle) database using connection details

"""
import os

from sqlalchemy import create_engine
from sqlalchemy.exc import DBAPIError

from gobcore.exceptions import GOBException
from gobimport.connector.config import DATABASE_CONFIGS, get_url


def connect_to_database(source):
    """Connect to the datasource

    The SQLAlchemy library is used to connect to the data source for databases

    :return: a connection to the given database
    """
    # Set the NLS_LANG variable to UTF-8 to get the correct encoding
    os.environ["NLS_LANG"] = ".UTF8"

    # Get the database config based on the source name
    name = source['application']
    try:
        DB = DATABASE_CONFIGS[name]
    except KeyError:
        raise GOBException(f"Database config for source {name} not found.")

    user = f"({DB['username']}@{DB['database']})"
    try:
        engine = create_engine(get_url(DB))
        connection = engine.connect()

    except DBAPIError as e:
        raise GOBException(f'Database connection for source {name} {user} failed. Error: {e}.')
    else:
        return connection, user
