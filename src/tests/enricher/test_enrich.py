import unittest
from unittest import mock

from gobimport.enricher import enrich

class TestEnricher(unittest.TestCase):

    def setUp(self):
        self.entities = []

    @mock.patch('gobimport.enricher._enrich_metingen')
    def test_valid_enrich(self, mock_enrich):
        enrich('meetbouten', 'metingen', self.entities)
        mock_enrich.assert_called()

    def test_invalid_enrich(self):
        enrich('test', 'test', self.entities)
