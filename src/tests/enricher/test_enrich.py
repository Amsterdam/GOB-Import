import unittest
from unittest import mock

from gobimport.enricher import enrich

class TestEnricher(unittest.TestCase):

    def setUp(self):
        self.entities = []
        self.log = lambda x: x
        pass

    @mock.patch('gobimport.enricher._enrich_metingen')
    def test_valid_enrich(self, mock_enrich):
        enrich('meetbouten', 'metingen', self.entities, self.log)
        mock_enrich.assert_called()

    def test_invalid_enrich(self):
        enrich('test', 'test', self.entities, self.log)
