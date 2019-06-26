import unittest
from unittest import mock

from gobimport.enricher.bag import BAGEnricher


@mock.patch("gobimport.enricher.bag.logger", mock.MagicMock())
class TestBAGEnrichment(unittest.TestCase):

    def test_enrich_nummeraanduidingen(self):
        entities = [
            {
                'ligt_in_bag_woonplaats': '1024;3594',
            },
            {
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
                'gebruiksdoel': '01|doel1;02|doel2',
                'gebruiksdoel_woonfunctie': '01|doel1',
                'gebruiksdoel_gezondheidszorg': '01|doel1',
                'toegang': '01|doel1',
                'pandidentificatie': "1234;5678",
                'nummeraanduidingid_neven': "1234;5678"
            }
        ]
        enricher = BAGEnricher("app", "bag", "verblijfsobjecten")
        for entity in entities:
            enricher.enrich(entity)

        self.assertEqual(entities[0]['gebruiksdoel'],[{'code': '01', 'omschrijving': 'doel1'}, {'code': '02', 'omschrijving': 'doel2'}])
        self.assertEqual(entities[0]['gebruiksdoel_woonfunctie'],{'code': '01', 'omschrijving': 'doel1'})
        self.assertEqual(entities[0]['gebruiksdoel_gezondheidszorg'],{'code': '01', 'omschrijving': 'doel1'})
        self.assertEqual(entities[0]['toegang'],[{'code': '01', 'omschrijving': 'doel1'}])
        self.assertEqual(entities[0]['pandidentificatie'],['1234', '5678'])
        self.assertEqual(entities[0]['nummeraanduidingid_neven'],['1234', '5678'])

    def test_enrich_dossiers(self):
        entities = [
            {
                'heeft_bag_brondocument': 'brondocument1;brondocument2',
            }
        ]
        enricher = BAGEnricher("app", "bag", "dossiers")
        for entity in entities:
            enricher.enrich(entity)

        self.assertEqual(entities[0]['heeft_bag_brondocument'],['brondocument1', 'brondocument2'])
