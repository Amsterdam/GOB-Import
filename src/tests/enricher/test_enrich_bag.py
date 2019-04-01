import unittest
from unittest import mock

from gobimport.enricher.bag import BAGEnricher


class TestBAGEnrichment(unittest.TestCase):

    def test_enrich_nummeraanduidingen(self):
        entities = [
            {
                'dossier': 'dossier1;dossier2',
            }
        ]

        enricher = BAGEnricher("bag", "nummeraanduidingen")
        for entity in entities:
            enricher.enrich(entity)

        self.assertEqual(entities[0]['dossier'],['dossier1', 'dossier2'])

    def test_enrich_verblijfsobjecten(self):
        entities = [
            {
                'dossier': 'dossier1;dossier2',
                'gebruiksdoel': '01|doel1;02|doel2',
                'gebruiksdoel_woonfunctie': '01|doel1',
                'gebruiksdoel_gezondheidszorg': '01|doel1',
                'toegang': '01|doel1',
                'pandidentificatie': "1234;5678"
            }
        ]
        enricher = BAGEnricher("bag", "verblijfsobjecten")
        for entity in entities:
            enricher.enrich(entity)

        self.assertEqual(entities[0]['dossier'],['dossier1', 'dossier2'])
        self.assertEqual(entities[0]['gebruiksdoel'],[{'code': '01', 'omschrijving': 'doel1'}, {'code': '02', 'omschrijving': 'doel2'}])
        self.assertEqual(entities[0]['gebruiksdoel_woonfunctie'],{'code': '01', 'omschrijving': 'doel1'})
        self.assertEqual(entities[0]['gebruiksdoel_gezondheidszorg'],{'code': '01', 'omschrijving': 'doel1'})
        self.assertEqual(entities[0]['toegang'],[{'code': '01', 'omschrijving': 'doel1'}])
        self.assertEqual(entities[0]['pandidentificatie'],['1234', '5678'])

    def test_enrich_panden(self):
        entities = [
            {
                'dossier': 'dossier1;dossier2',
            }
        ]
        enricher = BAGEnricher("bag", "panden")
        for entity in entities:
            enricher.enrich(entity)

        self.assertEqual(entities[0]['dossier'],['dossier1', 'dossier2'])
