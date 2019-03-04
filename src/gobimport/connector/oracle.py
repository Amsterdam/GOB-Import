"""Implementation of a database input connectors

The following connectors are implemented in this module:
    Database - Connects to a (oracle) database using connection details

"""
import os

import cx_Oracle

from gobcore.exceptions import GOBException
from gobimport.connector.config import DATABASE_CONFIGS


def connect_to_oracle(source):
    """Connect to the datasource

    The cx_Oracle library is used to connect to the data source for databases

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
    connection = cx_Oracle.Connection(f"{DB['username']}/{DB['password']}@{DB['host']}:{DB['port']}/{DB['database']}")
    return connection, user
