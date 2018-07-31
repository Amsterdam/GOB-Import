"""Abstract base class for an import gobimportclient

Import clients should derive from this class.
This allows for a generic control of imports

"""

from gobimportclient.connector import connect_to_file
from gobimportclient.converter import convert_from_file
from gobimportclient.message_broker import publish
import datetime


class ImportClient:
    """Main class for an import gobimportclient

    This class serves as an interface for every individual import gobimportclient

    """
    def __init__(self, config, dataset):
        self._config = config
        self._dataset = dataset

        self.source = self._dataset['source']

        self._data = None
        self._gob_data = None

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

        :return:
        """
        """Read the data from the data source

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
                "source_id": self._dataset['source']['entity_id'],
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
