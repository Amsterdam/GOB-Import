"""ImportClient class

An ImportClient is instantiated using a configuration and dataset definition.
The configuration is shared between import clients and contains for instance the message broker to
publish the results
The dataset is specific for each import client and tells for instance which fields should be extracted

The current implementation assumes csv-file based imports
"""

import datetime

from gobimportclient.connector import connect_to_file
from gobimportclient.converter import convert_from_file
from gobimportclient.message_broker import publish


class ImportClient:
    """Main class for an import client

    This class serves as the main client for which the import can be configured in a dataset.json

    """
    def __init__(self, config, dataset):
        self._config = config
        self._dataset = dataset

        self.source = self._dataset['source']

        self._data = None       # Holds the data in imput format
        self._gob_data = None   # Holds the imported data in GOB format

    def connect(self):
        """The first step of every import is a technical step. A connection need to be setup to
        connect to a database, filesystem, API, ...

        :return:
        """
        if self.source['type'] == "file":
            self._data = connect_to_file(config=self.source['config'])
        else:
            raise NotImplementedError

    def read(self):
        """Read the data from the data source

        :return:
        """
        if self.source['type'] == "file":
            #  No action required here, data is read by pandas in self._data
            pass
        else:
            raise NotImplementedError

    def convert(self):
        """Convert the input data to GOB format

        Todo: quality check (where should that be implemented) make sure no double id's are imported.

        :return:
        """
        if self.source['type'] == "file":
            self._gob_data = convert_from_file(self._data, dataset=self._dataset)
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
        result = {
            "header": {
                "version": self._dataset['version'],
                "entity": self._dataset['entity'],
                "entity_id": self._dataset['entity_id'],
                "source": self._dataset['source']['name'],
                "gob_model": self._dataset['gob_model'],
                "timestamp": datetime.datetime.now().isoformat(),
            },
            "summary": None,  # No log, metrics and qa indicators for now
            "contents": self._gob_data
        }
        self.publish_result(result)

    def publish_result(self, result):
        """Publish the results on the appropriate message broker queue

        :param result:
        :return:
        """
        publish(
            config=self._config["publisher"],
            key="fullimport.proposal",
            msg=result)
