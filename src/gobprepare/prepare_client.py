"""
Contains the PrepareClient class.

The PrepareClient reads a dataset definition with a source and destination database and the actions
to perform on the data. For now the only supported action is a data clone from an Oracle to a Postgres
database.
"""

import datetime
import traceback

from gobcore.logging.logger import logger
from gobimport.connector import connect_to_oracle
from gobprepare.cloner.oracle_to_postgres import OracleToPostgresCloner
from gobprepare.connector.postgresql import connect_to_postgresql

READ_BATCH_SIZE = 100000
WRITE_BATCH_SIZE = 100000


class PrepareClient:
    """
    PrepareClient

    Construct using a prepare definition and call start_prepare_client() to start the data preparation.

    Handles everything from reading prepare definition to calling the appropriate helpers.
    """
    _src_connection = None
    _src_user = None
    _dst_connection = None
    _dst_user = None

    def __init__(self, dataset):
        self._dataset = dataset
        self._action = dataset['action']
        self.source = self._dataset['source']
        self.source_app = self._dataset['source'].get('application', self._dataset['source']['schema'])
        self.destination = self._dataset['destination']
        self.destination_app = self._dataset['destination'].get(
            'application',
            self._dataset['destination']['schema']
        )

        start_timestamp = int(datetime.datetime.utcnow().replace(microsecond=0).timestamp())
        self.process_id = f"{start_timestamp}.{self.source_app}.{self.source['schema']}"
        extra_log_kwargs = {
            'process_id': self.process_id,
            'application': self.source.get('application'),
            'schema': self.source['schema'],
            'action': self._action
        }

        # Log start of import process
        logger.set_name("IMPORT")
        logger.set_default_args(extra_log_kwargs)
        logger.info(f"Prepare dataset {self.source['schema']} from {self.source_app} started")

    def connect(self):
        """Connects to data source and destination (postgres) database

        :return:
        """
        if self.source['type'] == "oracle":
            self._src_connection, self._src_user = connect_to_oracle(self.source)
        else:
            raise NotImplementedError

        logger.info(f"Connection to {self.source_app} {self._src_user} has been made.")

        # Destination database is always Postgres
        self._dst_connection, self._dst_user = connect_to_postgresql(self.destination)
        logger.info(f"Connection to {self.destination_app} {self._dst_user} has been made.")

    def clone(self):
        """Clones the source data using an external Cloner class.

        :return:
        """
        if self.source['type'] == "oracle":
            cloner = OracleToPostgresCloner(self._src_connection, self.source['schema'],
                                            self._dst_connection, self.destination['schema'],
                                            self._action)
        else:
            raise NotImplementedError

        cloner.clone()

    def prepare(self):
        """Starts the appropriate prepare action based on the input configuration

        :return:
        """
        if self._action["type"] == "clone":
            self.clone()
        else:
            raise NotImplementedError

    def start_prepare_process(self):
        """Entry method. Handles the prepare process.

        :return:
        """
        try:
            self.connect()
            self.prepare()
        except Exception as e:
            # Print error message, the message that caused the error and a short stacktrace
            stacktrace = traceback.format_exc(limit=-5)
            print("Prepare failed: {e}", stacktrace)
            # Log the error and a short error description
            logger.error(f'Prepare failed: {e}')
