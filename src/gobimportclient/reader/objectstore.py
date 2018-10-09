from objectstore.objectstore import get_full_container_list


def read_from_objectstore(connection, container_name):
    """Reads from the objectstore

    The Amsterdam/objectstore library is used to connect to the container

    :return: a list of data
    """
    result = get_full_container_list(connection, container_name)

    data = []
    for row in result:
        data.append(dict(row))

    return data
