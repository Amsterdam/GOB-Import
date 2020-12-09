import unittest

from gobimport.enricher.test_catalogue import TstCatalogueEnricher


class TestEnricher(unittest.TestCase):

    def test_enrich_rel_entity(self):
        entity = {
            'somekey': 'somevalue',
            'manyref_to_c': 'C1;C2',
            'manyref_to_d': 'D1;',
            'manyref_to_c_begin_geldigheid': 'some date;',
            'manyref_to_d_begin_geldigheid': 'some date;another date',
        }

        enricher = TstCatalogueEnricher('app name', 'test_catalogue', 'rel_test_entity_a')

        enricher.enrich_rel_entity(entity)
        self.assertEqual({
            'somekey': 'somevalue',
            'manyref_to_c': ['C1', 'C2'],
            'manyref_to_d': ['D1'],
            'manyref_to_c_begin_geldigheid': ['some date'],
            'manyref_to_d_begin_geldigheid': ['some date', 'another date'],
        }, entity)
