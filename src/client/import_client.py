from abc import ABC, abstractmethod

from client.message_broker import publish


class ImportClient(ABC):
    def __init__(self, config):
        self._config = config

    @abstractmethod
    def id(self):
        pass

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def read(self):
        pass

    @abstractmethod
    def convert(self):
        pass

    @abstractmethod
    def publish(self):
        pass

    def publish_results(self, results):
        publish(
            config=self._config["publisher"],
            queue=self._config["publisher"]["queue"],
            key="fullimport.proposal",
            msg=results)
