"""Test catalogue enrichment."""


from typing import Any

from gobimport.enricher.enricher import Enricher


class TstCatalogueEnricher(Enricher):
    """Test Catalog Enricher."""

    @classmethod
    def enriches(cls, app_name: str, catalog_name: str, entity_name: str) -> bool:
        """Enrich test_catalogue collections."""
        if catalog_name == "test_catalogue":
            enricher = TstCatalogueEnricher(app_name, catalog_name, entity_name)
            return enricher._enrich_entity is not None
        return False

    def __init__(self, app_name: str, catalogue_name: str, entity_name: str) -> None:
        """Initialise TstCatalogueEnricher."""
        super().__init__(
            app_name,
            catalogue_name,
            entity_name,
            methods={
                "rel_test_entity_a": self.enrich_rel_entity,
                "rel_test_entity_b": self.enrich_rel_entity,
            },
        )

    def enrich_rel_entity(self, entity: dict[str, Any]) -> None:
        """Enrich test_catalogue relations."""
        keys = ["manyref_to_c", "manyref_to_d", "manyref_to_c_begin_geldigheid", "manyref_to_d_begin_geldigheid"]

        for key in keys:
            try:
                entity[key] = [i for i in entity[key].split(";") if i]
            except (KeyError, AttributeError):
                pass
