"""
Abstract base class for any enrichment class

"""
from abc import ABC, abstractmethod


class Enricher(ABC):

    @classmethod
    @abstractmethod
    def enriches(cls, catalog_name, entity_name):
        """
        Tells wether this class enriches the given catalog - entity
        :param catalog_name:
        :param entity_name:
        :return:
        """
        pass

    def __init__(self, methods, entity_name):
        """
        Given the methods and entity name, the enrich_entity method is set

        :param methods:
        :param entity_name:
        """
        self._enrich_entity = methods.get(entity_name)

    def enrich(self, entity):
        """
        Enrich a single entity

        :param entity:
        :return:
        """
        if self._enrich_entity:
            self._enrich_entity(entity)
