import unittest
from unittest import mock

from gobimport.enricher.wkpb import WKPBEnricher


class TestWKPBEnrichment(unittest.TestCase):

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
