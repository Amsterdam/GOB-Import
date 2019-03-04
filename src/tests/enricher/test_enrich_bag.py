import unittest
from unittest import mock

from gobimport.enricher import bag


class TestBAGEnrichment(unittest.TestCase):

    def test_enrich_nummeraanduidingen(self):
        entities = [
            {
                'dossier': 'dossier1;dossier2',
            }
        ]
        bag._enrich_nummeraanduidingen(entities)

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
        bag._enrich_verblijfsobjecten(entities)

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
        bag._enrich_panden(entities)

        self.assertEqual(entities[0]['dossier'],['dossier1', 'dossier2'])
