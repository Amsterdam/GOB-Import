import re

from objectstore.objectstore import get_full_container_list


def read_from_objectstore(connection, container_name, file_filter=".*"):
    """Reads from the objectstore

    The Amsterdam/objectstore library is used to connect to the container

    :return: a list of data
    """
    result = get_full_container_list(connection, container_name)
    pattern = re.compile(file_filter)
    data = []
    for row in result:
        if pattern.match(row["name"]):
            data.append(dict(row))

    return data
