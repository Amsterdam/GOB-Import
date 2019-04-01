""" Enricher

This enricher calls some specific functions for collections to add missing values in the source.
"""
from gobimport.enricher.meetbouten import MeetboutenEnricher
from gobimport.enricher.gebieden import GebiedenEnricher
from gobimport.enricher.bag import BAGEnricher


class Enricher:

    def __init__(self, catalog_name, entity_name):
        """
        Select all applicable enrichers for the given catalog and entity

        :param catalog_name:
        :param entity_name:
        """
        self.enrichers = []
        for Enricher in [GebiedenEnricher, MeetboutenEnricher, BAGEnricher]:
            if Enricher.enriches(catalog_name, entity_name):
                self.enrichers.append(Enricher(catalog_name, entity_name))

    def enrich(self, entity):
        """
        Enrich the entity for all applicable enrichments
        :param entity:
        :return:
        """
        for enricher in self.enrichers:
            enricher.enrich(entity)
