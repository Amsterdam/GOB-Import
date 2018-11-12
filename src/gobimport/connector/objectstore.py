"""Implementation of Objectstore input connectors

The following connectors are implemented in this module:
    Objectstore - Connects to Objectstore using connection details

"""
from objectstore.objectstore import get_connection

from gobcore.exceptions import GOBException

from gobimport.connector.config import OBJECTSTORE_CONFIGS


def connect_to_objectstore(source):
    """Connect to the objectstore

    The Amsterdam/objectstore library is used to connect to the objectstore

    :return: a connection to the given objectstore
    """
    # Get the objectstore config based on the source name
    try:
        OBJECTSTORE = OBJECTSTORE_CONFIGS[source['name']]
    except KeyError:
        raise GOBException(f"Objectstore config for source {source['name']} not found.")

    user = f"({OBJECTSTORE['USER']}@{OBJECTSTORE['TENANT_NAME']})"
    try:
        connection = get_connection(OBJECTSTORE)

    except Exception as e:
        raise GOBException(f"Objectstore connection for source {source['name']} {user} failed. Error: {e}.")
    else:
        return connection, user
