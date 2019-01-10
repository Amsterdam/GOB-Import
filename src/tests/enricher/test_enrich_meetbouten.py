import decimal
import unittest
from unittest import mock

from gobimport.enricher.meetbouten import _enrich_metingen

class TestEnricher(unittest.TestCase):

    def setUp(self):
        self.entities = [
            {
                'identificatie': '1234',
                'hoort_bij_meetbout': '1',
                'datum': '2000-01-01',
                'hoogte_tov_nap': decimal.Decimal(0.1),
                'refereert_aan_refpunt': '123;456'
            }
        ]
        self.log = lambda x: x

        self.expected_attributes = [
            'type_meting',
            'hoeveelste_meting',
            'aantal_dagen',
            'zakking',
            'zakking_cumulatief',
            'zakkingssnelheid'
        ]

    def test_single_meting(self):
        _enrich_metingen(self.entities, self.log)

        for key in self.expected_attributes:
            self.assertIn(key, self.entities[0])

    def test_multiple_meting(self):
        self.entities.append(
            {
                'identificatie': '1235',
                'hoort_bij_meetbout': '1',
                'datum': '2000-01-11',
                'hoogte_tov_nap': decimal.Decimal(0.2)
            }
        )
        _enrich_metingen(self.entities, self.log)

        for key in self.expected_attributes:
            self.assertIn(key, self.entities[0])

        # Expect the first meting to be nieuw and meting #1
        self.assertEqual('N', self.entities[0]['type_meting'])
        self.assertEqual(1, self.entities[0]['hoeveelste_meting'])

        # Expect the second meting to be herhaal and meting #2
        self.assertEqual('H', self.entities[1]['type_meting'])
        self.assertEqual(2, self.entities[1]['hoeveelste_meting'])
        self.assertEqual(10, self.entities[1]['aantal_dagen'])
        self.assertEqual(-100.0, float(self.entities[1]['zakking_cumulatief']))
