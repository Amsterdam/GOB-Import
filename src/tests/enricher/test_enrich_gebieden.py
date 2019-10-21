import decimal
import unittest
from unittest import mock

from gobimport.enricher.gebieden import GebiedenEnricher, CBS_BUURTEN_API, CBS_WIJKEN_API


class MockResponse:

    @property
    def ok(self):
        return True

    def json(self):
        return {'features': [
            {
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[
                        [0,0],
                        [1,0],
                        [1,1],
                        [0,1],
                        [0,0]
                    ]]
                },
                'properties': {
                    'water': 'NEE',
                    'buurtcode': 'BU03630001',
                    'buurtnaam': 'Buurt'
                },
            },
            {
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[
                        [0,0],
                        [1,0],
                        [1,1],
                        [0,1],
                        [0,0]
                    ]]
                },
                'properties': {
                    'water': 'NEE',
                    'buurtcode': 'BU03630002',
                    'buurtnaam': 'Buurt dubbel'
                },
            },
            {
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[
                        [1,1],
                        [2,1],
                        [2,2],
                        [1,2],
                        [1,1]
                    ]]
                },
                'properties': {
                    'water': 'NEE',
                    'buurtcode': 'BU03630003',
                    'buurtnaam': 'Buurt anders'
                },
            },
            {
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[
                        [1,1],
                        [2,1],
                        [2,2],
                        [1,2],
                        [1,1]
                    ]]
                },
                'properties': {
                    'water': 'JA',
                    'buurtcode': 'BU03630004',
                    'buurtnaam': 'Buurt water'
                },
            }
        ]}

@mock.patch("gobimport.enricher.gebieden.logger", mock.MagicMock())
class TestEnricher(unittest.TestCase):

    def setUp(self):
        self.entities = [
            {
                'identificatie': '1234',
                'naam': 'Buurt',
                'eind_geldigheid': '',
                'geometrie': 'POLYGON((0 0,1 0,1 1,0 1,0 0))'
            },
            {
                'identificatie': '1235',
                'naam': 'Buurt',
                'eind_geldigheid': '',
                'geometrie': 'POLYGON((1 1,2 1,2 2,1 2,1 1))'
            },
            {
                'identificatie': '1236',
                'naam': 'Buurt',
                'eind_geldigheid': '2015-01-01',
                'geometrie': 'POLYGON((2 2,3 2,3 3,2 3,2 2))'
            }
        ]

    @mock.patch.object(GebiedenEnricher, '_add_cbs_code')
    def test_enrich_buurten(self, mock_add_cbs):
        enricher = GebiedenEnricher("app", "gebieden", "buurten")
        for entity in self.entities:
            enricher.enrich(entity)

        mock_add_cbs.assert_called_with(self.entities[2], CBS_BUURTEN_API, 'buurt')

    @mock.patch.object(GebiedenEnricher, '_add_cbs_code')
    def test_enrich_wijken(self, mock_add_cbs):
        enricher = GebiedenEnricher("app", "gebieden", "wijken")
        for entity in self.entities:
            enricher.enrich(entity)

        mock_add_cbs.assert_called_with(self.entities[2], CBS_WIJKEN_API, 'wijk')

    @mock.patch('gobimport.enricher.gebieden.requests.get')
    def test_add_cbs_code(self, mock_request):
        mock_request.return_value = MockResponse()
        enricher = GebiedenEnricher("app", "gebieden", "buurten")
        for entity in self.entities:
            enricher._add_cbs_code(entity, CBS_BUURTEN_API, 'buurt')

        # Expect cbs codes buurt
        self.assertEqual('BU03630001', self.entities[0]['cbs_code'])

        # Expect an empty string when datum_einde_geldigheid is not empty
        self.assertEqual('', self.entities[2]['cbs_code'])

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
