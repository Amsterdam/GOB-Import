from sqlalchemy.exc import DBAPIError

from gobcore.exceptions import GOBException


def read_from_database(connection, query):
    """Reads from the database

    The SQLAlchemy library is used to connect to the data source for databases

    :return: a list of data
    """
    try:
        result = connection.execute("\n".join(query)).fetchall()
    except DBAPIError as e:
        raise GOBException(f'Database connection failed. Error: {e}.')

    if len(result) == 0:
        raise GOBException('No results found for database query')

    data = []
    for row in result:
        data.append(dict(row))

    return data
