def read_from_database(connection, table):
    """Reads from the database

    The SQLAlchemy library is used to connect to the data source for databases

    :return: a list of data
    """

    statement = table.select()
    result = connection.execute(statement).fetchall()

    data = []
    for row in result:
        data.append(dict(row))

    return data
