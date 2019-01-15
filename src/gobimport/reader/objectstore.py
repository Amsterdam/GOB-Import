import re
import pandas
import io

from objectstore.objectstore import get_full_container_list, get_object


def read_from_objectstore(connection, config):
    """Reads from the objectstore

    The Amsterdam/objectstore library is used to connect to the container

    :return: a list of data
    """
    container_name = config["container"]
    file_filter = config.get("file_filter", ".*")
    file_type = config.get("file_type")

    result = get_full_container_list(connection, container_name)
    pattern = re.compile(file_filter)
    data = []
    for row in result:
        if pattern.match(row["name"]):
            if file_type == "XLS":
                # Include (non-empty) Excel rows
                obj = get_object(connection, row, container_name)
                data += _read_xls(obj)
            else:
                # Include file attributes
                data.append(dict(row))

    return data


def _read_xls(obj):
    """Read XLS(X) object

    Read Excel object and return the (non-empty) rows

    :param obj: An Objectstore object
    :return: The list of non-empty rows
    """
    io_obj = io.BytesIO(obj)
    excel = pandas.read_excel(io=io_obj)

    data = []
    for _, row in excel.iterrows():
        empty = True
        for key, value in row.items():
            # Convert any NULL values to None values
            if pandas.isnull(value):
                row[key] = None
            else:
                # Not NULL => row is not empty
                empty = False
        if not empty:
            data.append(row)

    return data
