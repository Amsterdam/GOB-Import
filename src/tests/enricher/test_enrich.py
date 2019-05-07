import unittest
from unittest import mock

from gobimport.enricher import Enricher, MeetboutenEnricher

class TestEnricher(unittest.TestCase):

    def setUp(self):
        self.entities = [{}]

    @mock.patch.object(MeetboutenEnricher, 'enrich_meting')
    def test_valid_enrich(self, mock_enrich):
        enricher = Enricher('app', 'meetbouten', 'metingen')
        for entity in self.entities:
            enricher.enrich(entity)
        mock_enrich.assert_called()

    def test_invalid_enrich(self):
        enricher = Enricher('app', 'test', 'test')
        for entity in self.entities:
            enricher.enrich(entity)
