"""
Reader

Contains logic to connect and read from a variety of datasources
"""
from gobcore.typesystem import GOB_SECURE_TYPES
from gobcore.model import GOBModel
from gobcore.secure.crypto import read_protect

from gobcore.logging.logger import logger
from gobcore.database.connector import (
    connect_to_database,
    connect_to_objectstore,
    connect_to_file,
    connect_to_oracle,
    connect_to_postgresql,
    connect_to_wfs
)
from gobcore.database.reader import (
    query_database,
    query_objectstore,
    query_file,
    query_oracle,
    query_postgresql,
    query_wfs
)
from gobimport.config import get_database_config, get_objectstore_config


class Reader:

    def __init__(self, source, app, dataset):
        """
        source:
        type :       type of source, e.g. file, database, ...
        application: name of the application or source that holds the data, e.g. Neuron, DIVA, ...
        query:       any query to run on the dataset that is being imported, e.g. a SQL query
        config:      any configuration parameters, e.g. encoding

        :param source: source definition object
        :param app: name of the import (often equal to source.application)
        """
        self.source = source
        self.app = app

        secure_types = [f"GOB.{type.name}" for type in GOB_SECURE_TYPES]
        mapping = dataset["gob_mapping"]

        catalogue = dataset['catalogue']
        entity = dataset['entity']
        gob_attributes = GOBModel().get_collection(catalogue, entity)["all_fields"]

        self.secure_attributes = []
        for maps_on, map_spec in mapping.items():
            gob_attr = gob_attributes[maps_on]
            if gob_attr["type"] in secure_types:
                self.secure_attributes.append(map_spec["source_mapping"])

        self._connection = None

    def connect(self):  # noqa: C901
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
        elif self.source['type'] == "wfs":
            self._connection, user = connect_to_wfs(self.source['url'])
        else:
            raise NotImplementedError

        logger.info(f"Connection to {self.app} {user} has been made.")

    def _protect_row(self, row):
        for attr in row.keys():
            if attr in self.secure_attributes:
                row[attr] = read_protect(row[attr])
        return row

    def _query(self, query):
        if self.secure_attributes:
            for result in query:
                yield self._protect_row(result)
        else:
            yield from query

    def read(self):  # noqa: C901
        """Read the data from the data source

        :return: iterable dataset
        """
        assert self._connection is not None, "No connection, connect should succeed before read"

        if self.source['type'] == "file":
            query = query_file(self._connection)
        elif self.source['type'] == "database":
            query = query_database(self._connection, self.source["query"])
        elif self.source['type'] == "oracle":
            query = query_oracle(self._connection, self.source["query"])
        elif self.source['type'] == "objectstore":
            query = query_objectstore(self._connection, self.source)
        elif self.source['type'] == "postgres":
            query = query_postgresql(self._connection, self.source["query"])
        elif self.source['type'] == "wfs":
            query = query_wfs(self._connection)
        else:
            raise NotImplementedError

        return self._query(query)
