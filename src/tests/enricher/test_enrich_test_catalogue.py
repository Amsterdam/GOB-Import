import unittest

from gobimport.enricher.test_catalogue import TstCatalogueEnricher


class TestEnricher(unittest.TestCase):

    def test_enrich_rel_entity(self):
        entity = {
            'somekey': 'somevalue',
            'manyref_to_c': 'C1;C2',
            'manyref_to_d': 'D1;',
        }

        enricher = TstCatalogueEnricher('app name', 'test_catalogue', 'rel_test_entity_a')

        enricher.enrich_rel_entity(entity)
        self.assertEqual(['C1', 'C2'], entity['manyref_to_c'])
        self.assertEqual(['D1'], entity['manyref_to_d'])
