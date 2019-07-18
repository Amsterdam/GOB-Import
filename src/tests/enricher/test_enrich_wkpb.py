import unittest
from unittest import mock

from gobimport.enricher.wkpb import WKPBEnricher


class TestWKPBEnrichment(unittest.TestCase):

    def test_enrich_beperkingen(self):
        entities = [
            {
                'belast_brk_kadastraal_object': 'ASD28|AH|1304|G|0;ASD28|AH|3839|G|0',
            }
        ]

        enricher = WKPBEnricher("app", "wkpb", "beperkingen")
        for entity in entities:
            enricher.enrich(entity)

        # Check if the values is split in an array
        self.assertEqual(entities[0]['belast_brk_kadastraal_object'], ['ASD28|AH|1304|G|0','ASD28|AH|3839|G|0'])

    def test_enrich_dossiers(self):
        entities = [
            {
                'heeft_wkpb_brondocument': 'BD00000016_WK00WK.pdf;BD00000016_WK01WK.pdf',
            }
        ]

        enricher = WKPBEnricher("app", "wkpb", "dossiers")
        for entity in entities:
            enricher.enrich(entity)

        # Check if the values is split in an array
        self.assertEqual(entities[0]['heeft_wkpb_brondocument'], ['BD00000016_WK00WK.pdf','BD00000016_WK01WK.pdf'])
