from gobimportclient.connector.database import connect_to_database
from gobimportclient.connector.objectstore import connect_to_objectstore
from gobimportclient.connector.file import connect_to_file

__all__ = ['connect_to_database', 'connect_to_objectstore', 'connect_to_file']
