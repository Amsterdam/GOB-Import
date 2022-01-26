import unittest
from unittest import mock
from unittest.mock import call

import pandas as pd

from gobimport.enricher.gebieden import GebiedenEnricher, CBS_CODES_WIJK, CBS_CODES_BUURT


class TestEnricher(unittest.TestCase):

    def setUp(self):
        self.entities = [
            {
                'identificatie': '1234',
                'code': '1234',
                'naam': 'Buurt',
                'eind_geldigheid': '',
                'geometrie': 'POLYGON((0 0,1 0,1 1,0 1,0 0))'
            },
            {
                'identificatie': '1235',
                'code': '1235',
                'naam': 'Buurt',
                'eind_geldigheid': '',
                'geometrie': 'POLYGON((1 1,2 1,2 2,1 2,1 1))'
            },
            {
                'identificatie': '1236',
                'code': '1236',
                'naam': 'Buurt',
                'eind_geldigheid': '2015-01-01',
                'geometrie': 'POLYGON((2 2,3 2,3 3,2 3,2 2))'
            }
        ]

    def test_enriches(self):
        self.assertTrue(GebiedenEnricher.enriches("app", "gebieden", "wijken"))
        self.assertFalse(GebiedenEnricher.enriches("app", "gebieden_false", "metingen"))

    @mock.patch.object(GebiedenEnricher, '_add_cbs_code')
    def test_enrich_buurten(self, mock_add_cbs):
        enricher = GebiedenEnricher("app", "gebieden", "buurten")
        for entity in self.entities:
            enricher.enrich(entity)

        mock_add_cbs.assert_called_with(self.entities[2], CBS_CODES_BUURT, 'buurt')

    @mock.patch.object(GebiedenEnricher, '_add_cbs_code')
    def test_enrich_wijken(self, mock_add_cbs):
        enricher = GebiedenEnricher("app", "gebieden", "wijken")
        for entity in self.entities:
            enricher.enrich(entity)

        mock_add_cbs.assert_called_with(self.entities[2], CBS_CODES_WIJK, 'wijk')

    @mock.patch('gobimport.enricher.gebieden.ObjectDatastore')
    def test_add_cbs_code(self, mock_datastore):
        mock_datastore.return_value.query.return_value = [pd.Series(["1234", "BU03630001", "Centrum"])]
        enricher = GebiedenEnricher("app", "gebieden", "buurten")

        enricher._add_cbs_code(self.entities[0], CBS_CODES_BUURT, 'buurt')

        mock_datastore.has_calls([call.connect(), call.query(''), call.disconnect()])
        # Expect cbs codes buurt
        self.assertEqual('BU03630001', self.entities[0]['cbs_code'])


class TestGGWPEnricher(unittest.TestCase):

    def setUp(self):
        self.entities = [
            {

            }
        ]

    def test_enrich_ggwgebieden(self):
        ggwgebieden = [
            {
                "GGW_BEGINDATUM": "YYYY-MM-DD HH:MM:SS",
                "GGW_EINDDATUM": "YYYY-MM-DD HH:MM:SS.fff",
                "GGW_DOCUMENTDATUM": "YYYY-MM-DD",
                "BUURTEN": "1, 2, 3",
                "_file_info": {
                    "last_modified": "2020-01-20T12:30:30.12345"
                }
            }
        ]
        enricher = GebiedenEnricher("app", "gebieden", "ggwgebieden")
        for entity in ggwgebieden:
            enricher.enrich(entity)
        self.assertEqual(ggwgebieden, [
            {
                "_IDENTIFICATIE": None,
                "registratiedatum": "2020-01-20T12:30:30.12345",
                "GGW_BEGINDATUM": "YYYY-MM-DD",
                "GGW_EINDDATUM": "YYYY-MM-DD",
                "GGW_DOCUMENTDATUM": "YYYY-MM-DD",
                "BUURTEN": ["1", "2", "3"],
                "_file_info": {"last_modified": "2020-01-20T12:30:30.12345"}
            }
        ])

    def test_enrich_ggpgebieden(self):
        ggpgebieden = [
            {
                "GGP_BEGINDATUM": "YYYY-MM-DD HH:MM:SS",
                "GGP_EINDDATUM": "YYYY-MM-DD HH:MM:SS.fff",
                "GGP_DOCUMENTDATUM": None,
                "BUURTEN": "1, 2, 3",
                "_file_info": {
                    "last_modified": "2020-01-20T12:30:30.12345"
                }
            }
        ]
        enricher = GebiedenEnricher("app", "gebieden", "ggpgebieden")
        for entity in ggpgebieden:
            enricher.enrich(entity)
        self.assertEqual(ggpgebieden, [
            {
                "_IDENTIFICATIE": None,
                "registratiedatum": "2020-01-20T12:30:30.12345",
                "GGP_BEGINDATUM": "YYYY-MM-DD",
                "GGP_EINDDATUM": "YYYY-MM-DD",
                "GGP_DOCUMENTDATUM": None,
                "BUURTEN": ["1", "2", "3"],
                "_file_info": {"last_modified": "2020-01-20T12:30:30.12345"}
            }
        ])
