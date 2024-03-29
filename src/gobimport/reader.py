"""Reader.

Contains logic to connect and read from a variety of data sources.
"""
from typing import Optional

from gobconfig.datastore.config import get_datastore_config
from gobcore.datastore.factory import Datastore, DatastoreFactory
from gobcore.enum import ImportMode
from gobcore.logging.logger import logger
from gobcore.secure.crypto import read_protect
from gobcore.typesystem import GOB_SECURE_TYPES

from gobimport import gob_model


class Reader:
    """Data source reader."""

    def __init__(self, source, app, dataset, mode: ImportMode = ImportMode.FULL):
        """Initialise Reader.

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

        catalogue = dataset["catalogue"]
        entity = dataset["entity"]
        gob_attributes = gob_model[catalogue]["collections"][entity]["all_fields"]
        self.secure_attributes: list[str] = []
        self.set_secure_attributes(mapping, gob_attributes)

        self.datastore: Optional[Datastore] = None

    def __enter__(self):
        """Enter Reader context."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit Reader context, always disconnect from datastore."""
        if self.datastore is not None:
            self.datastore.disconnect()
            logger.info(f"Disconnected from {self.app} {self.datastore.user}")

    def set_secure_attributes(self, mapping, gob_attributes) -> None:
        """Get the secure attributes so that they are read protected as soon as they are read.

        :param mapping:
        :param gob_attributes:
        :return:
        """
        for maps_on, map_spec in mapping.items():
            gob_attr = gob_attributes.get(maps_on)
            if gob_attr and gob_attr["type"] in self.secure_types:
                self.secure_attributes.append(map_spec["source_mapping"])
            if isinstance(map_spec["source_mapping"], dict) and gob_attr.get("secure"):
                # Dictionary of source mappings (normally a reference with secure attributes).
                source_mapping = {k: {"source_mapping": v} for k, v in map_spec["source_mapping"].items()}
                self.set_secure_attributes(source_mapping, gob_attr["secure"])

    def connect(self):  # noqa: C901
        """Connect to a database, filesystem, API, ...

        The first step of every import is a technical step.
        A connection needs to be set up.

        :return:
        """
        # Get manually added config, or config based on application name
        datastore_config = self.source.get("application_config") or get_datastore_config(self.source["application"])

        read_config = {**self.source.get("read_config", {}), "mode": self.mode}
        self.datastore = DatastoreFactory.get_datastore(datastore_config, read_config)
        self.datastore.connect()

        logger.info(f"Connection to {self.app} {self.datastore.user} has been made.")

    def _protect_row(self, row):
        for attr in row.keys():
            if attr in self.secure_attributes:
                row[attr] = read_protect(row[attr])
        return row

    def _maybe_protect_rows(self, query):
        if self.secure_attributes:
            for result in query:
                yield self._protect_row(result)
        else:
            yield from query

    def read(self):  # noqa: C901
        """Read the data from the data source.

        :return: iterable dataset
        """
        assert self.datastore is not None, (
            "No datastore, datastore should be initialised first. " "Have you called connect?"
        )

        # The source query is the query (only db-like connections have one)
        source_query = self.source.get("query", [])

        # Add partial query only if have source query, ignore for other datastores
        if source_query and self.mode != ImportMode.FULL:
            try:
                # Optionally populated with the mode, eg partial, random, ...
                source_query += self.source[self.mode.value]
            except KeyError as exc:
                logger.error(f"Unknown import mode for the collection: {self.mode.value}")
                raise exc

        # Name the cursor to activate server-side-cursor (only postgresql datastore)
        results = self.datastore.query("\n".join(source_query), arraysize=2000, name="import_cursor", withhold=True)

        return self._maybe_protect_rows(results)
