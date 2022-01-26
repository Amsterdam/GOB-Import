import io
import unittest
from unittest import mock

from gobimport.enricher.bag import BAGEnricher


@mock.patch("gobimport.enricher.bag.logger", mock.MagicMock())
@mock.patch("gobimport.enricher.bag.log_issue", mock.MagicMock())
class TestBAGEnrichment(unittest.TestCase):

    def test_enrich_nummeraanduidingen(self):
        entities = [
            {
                'identificatie': '1234',
                'ligt_in_bag_woonplaats': '1024;3594',
            },
            {
                'identificatie': '4321',
                'ligt_in_bag_woonplaats': '1024',
            }
        ]

        enricher = BAGEnricher("app", "bag", "nummeraanduidingen")
        for entity in entities:
            enricher.enrich(entity)

        # Check if the last value is selected
        self.assertEqual(entities[0]['ligt_in_bag_woonplaats'],'3594')
        self.assertEqual(entities[1]['ligt_in_bag_woonplaats'],'1024')

    def test_enrich_verblijfsobjecten(self):
        entities = [
            {
                'identificatie': '1234',
                'pandidentificatie': "1234;5678",
                'fng_code': 500
            },
            {
                'identificatie': '1234',
                'pandidentificatie': "1234;5678",
                'fng_code': 1000
            },
            {
                'identificatie': '1234',
                'pandidentificatie': "1234;5678",
            }
        ]
        enricher = BAGEnricher("app", "bag", "verblijfsobjecten")
        for entity in entities:
            enricher.enrich(entity)

        self.assertEqual(entities[0]['pandidentificatie'],['1234', '5678'])
        self.assertEqual(entities[0]['fng_omschrijving'],'Ongesubsidieerde bouw (500)')

        # Expect no fng_omschrijving with an invalid code
        self.assertEqual(entities[1]['fng_omschrijving'], None)

        # no fng_code and fng_omschrijving
        self.assertDictEqual(entities[2], {'identificatie': '1234', 'pandidentificatie': '1234;5678'})

    def test_enriches(self):
        # Expect a valid BAG collection to be enriched
        self.assertTrue(BAGEnricher.enriches("test", "bag", "nummeraanduidingen"))

        # Expect a invalid BAG collection to not be enriched
        self.assertFalse(BAGEnricher.enriches("test", "bag", "testcollection"))
