"""Implementation of a Postgresql input connector

The following connectors are implemented in this module:
    Postgresql - Connects to a postgres database using connection details

"""
import psycopg2
from gobcore.exceptions import GOBException
from gobimport.connector.config import DATABASE_CONFIGS


def connect_to_postgresql(source):
    # Get the database config based on the source name
    name = source['application']
    try:
        DB = DATABASE_CONFIGS[name]
    except KeyError:
        raise GOBException(f"Database config for source {name} not found.")

    user = f"({DB['username']}@{DB['database']})"
    try:
        connection = psycopg2.connect(
            database=DB['database'],
            user=DB['username'],
            password=DB['password'],
            host=DB['host'],
            port=DB['port'],
        )
    except psycopg2.OperationalError as e:
        raise GOBException(f'Database connection for source {name} {user} failed. Error: {e}.')
    else:
        return connection, user
