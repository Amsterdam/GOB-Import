"""
Testcatalogue enrichment

"""

from gobimport.enricher.enricher import Enricher


class TstCatalogueEnricher(Enricher):

    @classmethod
    def enriches(cls, app_name, catalog_name, entity_name):
        if catalog_name == "test_catalogue":
            enricher = TstCatalogueEnricher(app_name, catalog_name, entity_name)
            return enricher._enrich_entity is not None

    def __init__(self, app_name, catalogue_name, entity_name):
        super().__init__(app_name, catalogue_name, entity_name, methods={
            "rel_test_entity_a": self.enrich_rel_entity,
            "rel_test_entity_b": self.enrich_rel_entity,
        })

    def enrich_rel_entity(self, entity):
        keys = ['manyref_to_c', 'manyref_to_d']

        for key in keys:
            try:
                entity[key] = [i for i in entity[key].split(';') if i]
            except (KeyError, AttributeError) as e:
                pass
