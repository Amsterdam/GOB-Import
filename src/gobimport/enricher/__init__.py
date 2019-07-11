""" Enricher

This enricher calls some specific functions for collections to add missing values in the source.
"""
from gobimport.enricher.meetbouten import MeetboutenEnricher
from gobimport.enricher.gebieden import GebiedenEnricher
from gobimport.enricher.bag import BAGEnricher
from gobimport.enricher.wkpb import WKPBEnricher


class Enricher:

    def __init__(self, app_name, catalog_name, entity_name):
        """
        Select all applicable enrichers for the given catalog and entity

        :param catalog_name:
        :param entity_name:
        """
        self.enrichers = []
        for CatalogueEnricher in [GebiedenEnricher, MeetboutenEnricher, BAGEnricher, WKPBEnricher]:
            if CatalogueEnricher.enriches(app_name, catalog_name, entity_name):
                self.enrichers.append(CatalogueEnricher(app_name, catalog_name, entity_name))

    def enrich(self, entity):
        """
        Enrich the entity for all applicable enrichments
        :param entity:
        :return:
        """
        for enricher in self.enrichers:
            enricher.enrich(entity)
