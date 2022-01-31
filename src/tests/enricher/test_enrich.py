import unittest
from unittest import mock

from gobimport.enricher import BaseEnricher, MeetboutenEnricher, GebiedenEnricher


class TestEnricher(unittest.TestCase):

    def setUp(self):
        self.entities = [{}]

    @mock.patch.object(MeetboutenEnricher, 'enrich_meting')
    def test_valid_enrich(self, mock_enrich):
        enricher = BaseEnricher('app', 'meetbouten', 'metingen')
        for entity in self.entities:
            enricher.enrich(entity)
        mock_enrich.assert_called()

    @mock.patch.object(GebiedenEnricher, 'enrich_buurt')
    def test_valid_enrich(self, mock_enrich):
        enricher = BaseEnricher('app', 'gebieden', 'buurten')
        for entity in self.entities:
            enricher.enrich(entity)
        mock_enrich.assert_called()

    def test_invalid_enrich(self):
        enricher = BaseEnricher('app', 'test', 'test')
        for entity in self.entities:
            enricher.enrich(entity)
        self.assertListEqual(enricher.enrichers, [])
