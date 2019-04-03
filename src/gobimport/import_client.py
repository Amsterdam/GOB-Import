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

from gobcore.database.connector import connect_to_database, connect_to_objectstore, connect_to_file, connect_to_oracle
from gobcore.database.reader import query_database, query_objectstore, query_file, query_oracle
from gobcore.logging.logger import logger
from gobcore.message_broker import publish
from gobcore.message_broker.offline_contents import ContentsWriter

from gobimport.converter import Converter
from gobimport.injections import Injector
from gobimport.config import get_database_config, get_objectstore_config
from gobimport.validator import Validator
from gobimport.enricher import Enricher
from gobimport.entity_validator import EntityValidator


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

        # Find the functional source id
        # This is the functional field that is mapped onto the source_id
        # or _source_id if no mapping exists
        ids = [key for key, value in self._dataset["gob_mapping"].items() if value["source_mapping"] == self.source_id]
        self.func_source_id = ids[0] if ids else "_source_id"

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
        self.validator = Validator(self.entity, self.source_id)
        self.converter = Converter(self.catalogue, self.entity, self._dataset)
        self.entity_validator = EntityValidator(self.catalogue, self.entity, self.func_source_id)

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
            self._connection, self._user = connect_to_database(get_database_config(self.source['application']))
        elif self.source['type'] == "oracle":
            self._connection, self._user = connect_to_oracle(get_database_config(self.source['application']))
        elif self.source['type'] == "objectstore":
            self._connection, self._user = connect_to_objectstore(get_objectstore_config(self.source['application']))
        else:
            raise NotImplementedError

        logger.info(f"Connection to {self.source_app} {self._user} has been made.")

    def read(self):
        """Read the data from the data source

        :return:
        """
        if self.source['type'] == "file":
            self._data = query_file(self._connection)
        elif self.source['type'] == "database":
            self._data = query_database(self._connection, self.source["query"])
        elif self.source['type'] == "oracle":
            self._data = query_oracle(self._connection, self.source["query"])
        elif self.source['type'] == "objectstore":
            self._data = query_objectstore(self._connection, self.source)
        else:
            raise NotImplementedError

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
            'num_records': self.n_rows
        }

        # Log end of import process
        logger.info(f"Import dataset {self.entity} from {self.source_app} completed. "
                    f"{summary['num_records']} records were read from the source.",
                    kwargs={"data": summary})

        import_message = {
            "header": metadata,
            "summary": summary,
            "contents_ref": self.filename
        }
        publish("gob.workflow.proposal", "fullimport.proposal", import_message)

    def start_import_process(self):
        try:
            self.connect()
            self.read()

            with ContentsWriter() as writer:
                self.filename = writer.filename

                self.n_rows = 0
                for row in self._data:
                    self.n_rows += 1

                    self.injector.inject(row)

                    self.enricher.enrich(row)

                    self.validator.validate(row)

                    entity = self.converter.convert(row)

                    self.entity_validator.validate(entity)

                    writer.write(entity)

                self.validator.result()
                self.entity_validator.result()

            logger.info(f"Data ({self.n_rows} records) has been imported from {self.source_app}.")

            self.publish()
        except Exception as e:
            # Print error message, the message that caused the error and a short stacktrace
            stacktrace = traceback.format_exc(limit=-5)
            print("Import failed: {e}", stacktrace)
            # Log the error and a short error description
            logger.error(f'Import failed: {e}')
        finally:
            self.clear_data()
