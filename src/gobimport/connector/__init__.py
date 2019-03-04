from gobimport.connector.database import connect_to_database
from gobimport.connector.oracle import connect_to_oracle
from gobimport.connector.objectstore import connect_to_objectstore
from gobimport.connector.file import connect_to_file

__all__ = ['connect_to_database', 'connect_to_objectstore', 'connect_to_file', 'connect_to_oracle']
