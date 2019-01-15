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

from gobcore.log import get_logger
from gobcore.message_broker import publish

from gobimport.converter import convert_data
from gobimport.injections import inject
from gobimport.connector import connect_to_database, connect_to_objectstore, connect_to_file
from gobimport.reader import read_from_database, read_from_objectstore, read_from_file
from gobimport.validator import Validator
from gobimport.enricher import enrich


logger = get_logger(name="IMPORT")


class ImportClient:
    """Main class for an import client

    This class serves as the main client for which the import can be configured in a dataset.json

    """
    def __init__(self, dataset):
        self._dataset = dataset
        self.source = self._dataset['source']
        self.source_id = self._dataset['source']['entity_id']
        self.source_app = self._dataset['source'].get('application', self._dataset['source']['name'])
        self.catalogue = self._dataset['catalogue']
        self.entity = self._dataset['entity']

        # Extra variables for logging
        start_timestamp = int(datetime.datetime.now().replace(microsecond=0).timestamp())
        self.process_id = f"{start_timestamp}.{self.source_app}.{self.entity}"
        self.extra_log_kwargs = {
            'process_id': self.process_id,
            'source': self.source['name'],
            'application': self.source.get('application'),
            'catalogue': self.catalogue,
            'entity': self.entity
        }

        # Log start of import process
        self.log(level='info',
                 msg=f"Import dataset {self.entity} from {self.source_app} started")

        self._connection = None     # Holds the connection to the source
        self._user = None           # Holds the user that connects to the source, eg user@database
        self._data = None           # Holds the data in imput format
        self._gob_data = None       # Holds the imported data in GOB format

    def log(self, level, msg, extra_info={}):
        log_func = getattr(logger, level)
        log_func(msg, extra={**self.extra_log_kwargs, **extra_info})

    def connect(self):
        """The first step of every import is a technical step. A connection need to be setup to
        connect to a database, filesystem, API, ...

        :return:
        """
        if self.source['type'] == "file":
            self._connection, self._user = connect_to_file(config=self.source['config'])
        elif self.source['type'] == "database":
            self._connection, self._user = connect_to_database(self.source)
        elif self.source['type'] == "objectstore":
            self._connection, self._user = connect_to_objectstore(self.source)
        else:
            raise NotImplementedError

        self.log(level='info',
                 msg=f"Connection to {self.source_app} {self._user} has been made.")

    def read(self):
        """Read the data from the data source

        :return:
        """
        if self.source['type'] == "file":
            self._data = read_from_file(self._connection)
        elif self.source['type'] == "database":
            self._data = read_from_database(self._connection, self.source["query"])
        elif self.source['type'] == "objectstore":
            self._data = read_from_objectstore(self._connection, self.source)
        else:
            raise NotImplementedError

        self.log(level='info',
                 msg=f"Data ({len(self._data)} records) has been imported from {self.source_app}.")

    def inject(self):
        inject_spec = self.source.get("inject")
        if inject_spec:
            self.log(level='info', msg="Inject conversion data")
            inject(inject_spec, self._data)

    def enrich(self):
        self.log(level='info', msg="Enrich")
        enrich(self.catalogue, self.entity, self._data, self.log)

    def convert(self):
        """Convert the input data to GOB format

        Todo: quality check (where should that be implemented) make sure no double id's are imported.

        :return:
        """
        # Convert the input data to GOB data using the import mapping
        self.log(level='info', msg="Convert")
        self._gob_data = convert_data(self._data, dataset=self._dataset)

    def validate(self):
        self.log(level='info', msg="Validate")
        validator = Validator(self, self._dataset['entity'], self._data, self.source_id)
        validator.validate()

    def publish(self):
        """The result of the import needs to be published.

        Publication includes a header, summary and results
        The header is for identification purposes
        The summary is for the interpretation of the results. Was the import successful, what er the metrics, etc
        The results is the imported data in GOB format

        :return:
        """
        metadata = {
            "process_id": self.process_id,
            "source": self._dataset['source']['name'],
            "application": self._dataset['source'].get('application'),
            "depends_on": self._dataset['source'].get('depends_on', {}),
            "enrich": self._dataset['source'].get('enrich', {}),
            "catalogue": self._dataset['catalogue'],
            "entity": self._dataset['entity'],
            "version": self._dataset['version'],
            "timestamp": datetime.datetime.now().isoformat()
        }

        summary = {
            'num_records': len(self._gob_data)
        }

        # Log end of import process
        self.log(level='info',
                 msg=f"Import dataset {self.entity} from {self.source_app} completed. "
                     f"{summary['num_records']} records were read from the source.",
                 extra_info={"data": summary})

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
            self.publish()
        except Exception as e:
            # Print error message, the message that caused the error and a short stacktrace
            stacktrace = traceback.format_exc(limit=-5)
            print("Import failed: {e}", stacktrace)
            # Log the error and a short error description
            self.log(level='error', msg=f'Import failed: {e}')
