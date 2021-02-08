"""
Reader

Contains logic to connect and read from a variety of datasources
"""
from gobcore.typesystem import GOB_SECURE_TYPES
from gobcore.enum import ImportMode
from gobcore.model import GOBModel
from gobcore.secure.crypto import read_protect

from gobcore.logging.logger import logger
from gobconfig.datastore.config import get_datastore_config
from gobcore.datastore.factory import DatastoreFactory


class Reader:

    def __init__(self, source, app, dataset, mode: ImportMode = ImportMode.FULL):
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
        self.mode = mode

        self.secure_types = [f"GOB.{type.name}" for type in GOB_SECURE_TYPES]
        mapping = dataset["gob_mapping"]

        catalogue = dataset['catalogue']
        entity = dataset['entity']
        gob_attributes = GOBModel().get_collection(catalogue, entity)["all_fields"]

        self.secure_attributes = []
        self.set_secure_attributes(mapping, gob_attributes)

        self.datastore = None

    def set_secure_attributes(self, mapping, gob_attributes):
        """
        Get the secure attributes so that they are read protected as soon as they are read

        :param mapping:
        :param gob_attributes:
        :return:
        """
        for maps_on, map_spec in mapping.items():
            gob_attr = gob_attributes.get(maps_on)
            if gob_attr and gob_attr["type"] in self.secure_types:
                self.secure_attributes.append(map_spec["source_mapping"])
            if isinstance(map_spec['source_mapping'], dict) and gob_attr.get('secure'):
                # dictionary of source mappings (normally a reference with secure attributes
                source_mapping = {k: {'source_mapping': v} for k, v in map_spec['source_mapping'].items()}
                self.set_secure_attributes(source_mapping, gob_attr['secure'])

    def connect(self):  # noqa: C901
        """The first step of every import is a technical step. A connection need to be setup to
        connect to a database, filesystem, API, ...

        :return:
        """

        # Get manually added config, or config based on application name
        datastore_config = self.source.get('application_config') or get_datastore_config(self.source['application'])

        read_config = {**self.source.get('read_config', {}), 'mode': self.mode}
        self.datastore = DatastoreFactory.get_datastore(datastore_config, read_config)
        self.datastore.connect()

        logger.info(f"Connection to {self.app} {self.datastore.user} has been made.")

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
        assert self.datastore is not None, "No datastore, datastore should be initialised first. " \
                                           "Have you called connect?"

        # The source query is the query (only db-like connections have one)
        source_query = self.source.get("query", [])

        # Add partial query only if have source query, ignore for other datastores
        if source_query and self.mode != ImportMode.FULL:
            try:
                # Optionally populated with the mode, eg partial, random, ...
                source_query += self.source[self.mode.value]
            except KeyError as e:
                logger.error(f"Unknown import mode for the collection: '{self.mode.value}'")
                raise e

        return self._query(self.datastore.query("\n".join(source_query)))
