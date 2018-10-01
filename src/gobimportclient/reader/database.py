def read_from_database(connection, query):
    """Reads from the database

    The SQLAlchemy library is used to connect to the data source for databases

    :return: a list of data
    """

    result = connection.execute(query).fetchall()

    data = []
    for row in result:
        data.append(dict(row))

    return data
