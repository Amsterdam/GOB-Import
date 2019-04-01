"""ImportClient class

An ImportClient is instantiated using a configuration and dataset definition.
The configuration is shared between import clients and contains for instance the message broker to
publish the results
The dataset is specific for each import client and tells for instance which fields should be extracted

The current implementation assumes csv-file based imports

Todo: improve type conversion
    e.g. for bools the true and false values are hardcoded.
    N = False, else is True, but this can vary per import
"""

import datetime
import traceback

from gobcore.logging.logger import logger
from gobcore.message_broker import publish

from gobimport.converter import convert_data
from gobimport.injections import Injector
from gobimport.connector import connect_to_database, connect_to_objectstore, connect_to_file, connect_to_oracle
from gobimport.reader import read_from_database, read_from_objectstore, read_from_file, read_from_oracle
from gobimport.validator import Validator
from gobimport.enricher import Enricher
from gobimport.entity_validator import entity_validate


class ImportClient:
    """Main class for an import client

    This class serves as the main client for which the import can be configured in a dataset.json

    """
    def __init__(self, dataset, msg):
        self.header = msg.get("header", {})
        self._dataset = dataset
        self.source = self._dataset['source']
        self.source_id = self._dataset['source']['entity_id']
        self.source_app = self._dataset['source'].get('application', self._dataset['source']['name'])
        self.catalogue = self._dataset['catalogue']
        self.entity = self._dataset['entity']

        # Extra variables for logging
        start_timestamp = int(datetime.datetime.utcnow().replace(microsecond=0).timestamp())
        self.process_id = f"{start_timestamp}.{self.source_app}.{self.entity}"
        extra_log_kwargs = {
            'process_id': self.process_id,
            'source': self.source['name'],
            'application': self.source.get('application'),
            'catalogue': self.catalogue,
            'entity': self.entity
        }

        # Log start of import process
        logger.set_name("IMPORT")
        logger.set_default_args(extra_log_kwargs)
        logger.info(f"Import dataset {self.entity} from {self.source_app} started")

        self.clear_data()

        self.injector = Injector(self.source.get("inject"))
        self.enricher = Enricher(self.catalogue, self.entity)

    def clear_data(self):
        """
        Clears local data

        :return: None
        """
        self._connection = None     # Holds the connection to the source
        self._user = None           # Holds the user that connects to the source, eg user@database
        self._data = None           # Holds the data in imput format
        self._gob_data = None       # Holds the imported data in GOB format

    def connect(self):
        """The first step of every import is a technical step. A connection need to be setup to
        connect to a database, filesystem, API, ...

        :return:
        """
        if self.source['type'] == "file":
            self._connection, self._user = connect_to_file(config=self.source['config'])
        elif self.source['type'] == "database":
            self._connection, self._user = connect_to_database(self.source)
        elif self.source['type'] == "oracle":
            self._connection, self._user = connect_to_oracle(self.source)
        elif self.source['type'] == "objectstore":
            self._connection, self._user = connect_to_objectstore(self.source)
        else:
            raise NotImplementedError

        logger.info(f"Connection to {self.source_app} {self._user} has been made.")

    def read(self):
        """Read the data from the data source

        :return:
        """
        if self.source['type'] == "file":
            self._data = read_from_file(self._connection)
        elif self.source['type'] == "database":
            self._data = read_from_database(self._connection, self.source["query"])
        elif self.source['type'] == "oracle":
            self._data = read_from_oracle(self._connection, self.source["query"])
        elif self.source['type'] == "objectstore":
            self._data = read_from_objectstore(self._connection, self.source)
        else:
            raise NotImplementedError

        logger.info(f"Data ({len(self._data)} records) has been imported from {self.source_app}.")

    def inject(self):
        for row in self._data:
            self.injector.inject(row)

    def enrich(self):
        for row in self._data:
            self.enricher.enrich(row)

    def convert(self):
        """Convert the input data to GOB format

        Todo: quality check (where should that be implemented) make sure no double id's are imported.

        :return:
        """
        # Convert the input data to GOB data using the import mapping
        logger.info("Convert")
        self._gob_data = convert_data(self._data, dataset=self._dataset)

    def validate(self):
        logger.info("Validate")
        validator = Validator(self._dataset['entity'], self._data, self.source_id)
        validator.validate()

    def entity_validate(self):
        logger.info("Validate Entity")
        # Find the functional source id
        # This is the functional field that is mapped onto the source_id
        # or _source_id if no mapping exists
        ids = [key for key, value in self._dataset["gob_mapping"].items() if value["source_mapping"] == self.source_id]
        func_source_id = ids[0] if ids else "_source_id"
        entity_validate(self.catalogue, self.entity, self._gob_data, func_source_id)

    def publish(self):
        """The result of the import needs to be published.

        Publication includes a header, summary and results
        The header is for identification purposes
        The summary is for the interpretation of the results. Was the import successful, what er the metrics, etc
        The results is the imported data in GOB format

        :return:
        """
        metadata = {
            **self.header,
            "process_id": self.process_id,
            "source": self._dataset['source']['name'],
            "application": self._dataset['source'].get('application'),
            "depends_on": self._dataset['source'].get('depends_on', {}),
            "enrich": self._dataset['source'].get('enrich', {}),
            "catalogue": self._dataset['catalogue'],
            "entity": self._dataset['entity'],
            "version": self._dataset['version'],
            "timestamp": datetime.datetime.utcnow().isoformat()
        }

        summary = {
            'num_records': len(self._gob_data)
        }

        # Log end of import process
        logger.info(f"Import dataset {self.entity} from {self.source_app} completed. "
                    f"{summary['num_records']} records were read from the source.",
                    kwargs={"data": summary})

        import_message = {
            "header": metadata,
            "summary": summary,
            "contents": self._gob_data
        }
        publish("gob.workflow.proposal", "fullimport.proposal", import_message)

    def start_import_process(self):
        try:
            self.connect()
            self.read()
            self.inject()
            self.enrich()
            self.validate()
            self.convert()
            self.entity_validate()
            self.publish()
        except Exception as e:
            # Print error message, the message that caused the error and a short stacktrace
            stacktrace = traceback.format_exc(limit=-5)
            print("Import failed: {e}", stacktrace)
            # Log the error and a short error description
            logger.error(f'Import failed: {e}')
        finally:
            self.clear_data()
