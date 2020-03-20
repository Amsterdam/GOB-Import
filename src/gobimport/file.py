import pandas


def connect_to_file(config, file_path):
    """Connect to the datasource

    The pandas library is used to connect to the data source for CSV data files

    :return:
    """
    user = ""   # No user identification for file reads
    if config['filetype'] == "CSV":
        return pandas.read_csv(
            filepath_or_buffer=file_path,
            sep=config['separator'],
            encoding=config['encoding'],
            dtype=str), user
    else:
        raise NotImplementedError


def query_file(connection):
    """Reads from the file connection

    The pandas library is used to iterate through the items

    :return: a list of dicts
    """
    def convert_row(row):
        def convert(v):
            return None if pandas.isnull(v) else v

        return {k: convert(v) for k, v in row.items()}

    for _, row in connection.iterrows():
        yield convert_row(row)


def read_from_file(connection):
    """Reads from the file connection

    The pandas library is used to iterate through the items

    :return: a list of dicts
    """
    return [row for row in query_file(connection)]
