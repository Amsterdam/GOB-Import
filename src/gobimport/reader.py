"""
Reader

Contains logic to connect and read from a variety of datasources
"""
from gobcore.logging.logger import logger
from gobcore.database.connector import (
    connect_to_database,
    connect_to_objectstore,
    connect_to_file,
    connect_to_oracle,
    connect_to_postgresql
)
from gobcore.database.reader import query_database, query_objectstore, query_file, query_oracle, query_postgresql
from gobimport.config import get_database_config, get_objectstore_config


class Reader:

    def __init__(self, source, app):
        """
        source:
        type :       type of source, e.g. file, database, ...
        application: name of the application or source that holds the data, e.g. Neuron, DIVA, ...
        query:       any query to run on the dataset that is being imported, e.g. a SQL query
        config:      any configuration parameters, e.g. encoding

        :param source: source definition object
        :param app: name of the import (often equal to source.application)
        """
        keys = ['type', 'application', 'query', 'config']
        self.source = {key: value for key, value in source.items() if key in keys}
        # self.source = source
        self.app = app

        self._connection = None

    def connect(self):
        """The first step of every import is a technical step. A connection need to be setup to
        connect to a database, filesystem, API, ...

        :return:
        """
        if self.source['type'] == "file":
            self._connection, user = connect_to_file(config=self.source['config'])
        elif self.source['type'] == "database":
            self._connection, user = connect_to_database(get_database_config(self.source['application']))
        elif self.source['type'] == "oracle":
            self._connection, user = connect_to_oracle(get_database_config(self.source['application']))
        elif self.source['type'] == "objectstore":
            self._connection, user = connect_to_objectstore(get_objectstore_config(self.source['application']))
        elif self.source['type'] == "postgres":
            self._connection, user = connect_to_postgresql(get_database_config(self.source['application']))
        else:
            raise NotImplementedError

        logger.info(f"Connection to {self.app} {user} has been made.")

    def read(self):
        """Read the data from the data source

        :return: iterable dataset
        """
        assert self._connection is not None, "No connection, connect should succeed before read"

        if self.source['type'] == "file":
            return query_file(self._connection)
        elif self.source['type'] == "database":
            return query_database(self._connection, self.source["query"])
        elif self.source['type'] == "oracle":
            return query_oracle(self._connection, self.source["query"])
        elif self.source['type'] == "objectstore":
            return query_objectstore(self._connection, self.source)
        elif self.source['type'] == "postgres":
            return query_postgresql(self._connection, self.source["query"])
        else:
            raise NotImplementedError
