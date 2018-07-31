"""Abstract base class for an import gobimportclient

Import clients should derive from this class.
This allows for a generic control of imports

"""
from abc import ABC, abstractmethod

from gobimportclient.message_broker import publish


class ImportClient(ABC):
    """Abstract base class for an import gobimportclient

    This class serves as an interface that needs to be implemented for every individual import gobimportclient

    """
    def __init__(self, config):
        self._config = config

    @abstractmethod
    def id(self):
        """Every import gobimportclient should be able to identify itself

        :return: The identification of the import gobimportclient, e.g. Meetboutengis - meetbouten
        """
        pass

    @abstractmethod
    def connect(self):
        """The first step of every import is a technical step. A connection need to be setup to
        connect to a database, filesystem, API, ...

        :return:
        """
        pass

    @abstractmethod
    def read(self):
        """The next step is to read the data from the external source

        :return:
        """
        pass

    @abstractmethod
    def convert(self):
        """The data that is read by the read method needs to be converted to GOB format

        :return:
        """
        pass

    @abstractmethod
    def publish(self):
        """The result of the import needs to be published.

        Publication includes a header, summary and results
        The header is for identification purposes
        The summary is for the interpretation of the results. Was the import successful, what er the metrics, etc
        The results is the imported data in GOB format

        :return:
        """
        pass

    def publish_result(self, result):
        """Publish the results on the appropriate message broker queue

        :param result:
        :return:
        """
        publish(
            config=self._config["publisher"],
            key="fullimport.proposal",
            msg=result)
