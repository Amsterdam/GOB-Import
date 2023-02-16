"""Enricher.

This enricher calls some specific functions for collections to add missing values in the source.
"""


from typing import Any

from gobimport.enricher.bag import BAGEnricher
from gobimport.enricher.gebieden import GebiedenEnricher
from gobimport.enricher.meetbouten import MeetboutenEnricher
from gobimport.enricher.test_catalogue import TstCatalogueEnricher


class BaseEnricher:
    """Base Enricher."""

    def __init__(self, app_name: str, catalog_name: str, entity_name: str) -> None:
        """Select all applicable enrichers for the given catalog and entity.

        :param catalog_name:
        :param entity_name:
        """
        self.enrichers = []
        for CatalogueEnricher in [GebiedenEnricher, MeetboutenEnricher, BAGEnricher, TstCatalogueEnricher]:
            if CatalogueEnricher.enriches(app_name, catalog_name, entity_name):
                self.enrichers.append(CatalogueEnricher(app_name, catalog_name, entity_name))  # type: ignore[abstract]

    def enrich(self, entity: dict[str, Any]) -> None:
        """Enrich the entity for all applicable enrichments.

        :param entity:
        :return:
        """
        for enricher in self.enrichers:
            enricher.enrich(entity)
