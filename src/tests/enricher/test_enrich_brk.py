from unittest import TestCase

from gobimport.enricher.brk import BRKEnricher


class TestBRKEnricher(TestCase):

    def test_enriches(self):
        result = BRKEnricher.enriches('app name', 'catalog name', 'entity name')
        self.assertIsNone(result)

        result = BRKEnricher.enriches('app name', 'brk', 'entity name')
        self.assertFalse(result)

        result = BRKEnricher.enriches('app name', 'brk', 'gemeentes')
        self.assertTrue(result)

    def test_enrich_gemeentes(self):
        enricher = BRKEnricher('app name', 'brk', 'gemeentes')

        gemeente = {
            'properties': {
                'code': 'CODE',
                'gemeentenaam': 'NAAM'
            }
        }

        enricher.enrich_gemeentes(gemeente)
        self.assertEqual({
            'properties': {
                'code': 'CODE',
                'gemeentenaam': 'NAAM'
            },
            'code': 'CODE',
            'gemeentenaam': 'NAAM'
        }, gemeente)
