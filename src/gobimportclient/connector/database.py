"""Implementation of a database input connectors

The following connectors are implemented in this module:
    Database - Connects to a (oracle) database using connection details

"""
from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import DBAPIError

from gobcore.exceptions import GOBException

from gobimportclient.connector.config import DATABASE_CONFIGS


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

    try:
        engine = create_engine(URL(**DB))
        connection = engine.connect()

        db_schema = source['schema']
        table_name_suffix = f'{db_schema}.' if db_schema else ''
        table_name = f"{table_name_suffix}{source['table']}"

        # Create a metadata instance
        metadata = MetaData(engine, schema=db_schema)
        metadata.reflect(bind=engine, only=['meetbout'])

        # Get the required table from the metadata
        table = metadata.tables[table_name]
    except DBAPIError as e:
        raise GOBException(f'Database connection failed. Error: {e}.')
    else:
        return connection, table
