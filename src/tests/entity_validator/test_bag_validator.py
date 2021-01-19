import datetime
import unittest
from unittest.mock import MagicMock, patch, call

from gobcore.typesystem import GOB
from gobimport.entity_validator.bag import BAGValidator
from gobcore.quality.issue import QA_CHECK


class TestEntityValidator(unittest.TestCase):

    def setUp(self):
        self.entities = []

    @patch("gobimport.entity_validator.bag.log_issue")
    def test_validate_panden_valid(self, mock_log_issue):
        self.entities = [
            {
                'identificatie': '03631',
                'aantal_bouwlagen': 11,
                'hoogste_bouwlaag': 10,
                'laagste_bouwlaag': 0
            },
            {
                'identificatie': '03632',
                'aantal_bouwlagen': 5,
                'hoogste_bouwlaag': 10,
                'laagste_bouwlaag': 5
            },
            {
                'identificatie': '03633',
                'aantal_bouwlagen': 12,
                'hoogste_bouwlaag': 10,
                'laagste_bouwlaag': -1
            },
            {
                'identificatie': '0457', # Will be skipped
                'aantal_bouwlagen': 0,
                'hoogste_bouwlaag': 10,
                'laagste_bouwlaag': 0
            },
            {
                'identificatie': '0424', # Will be skipped
                'aantal_bouwlagen': 0,
                'hoogste_bouwlaag': 10,
                'laagste_bouwlaag': 0
            }
        ]
        validator = BAGValidator("bag", "panden")
        for entity in self.entities:
            validator.validate(entity)
        self.assertTrue(validator.result())

        self.assertEqual(mock_log_issue.call_count, 0)

    @patch("gobimport.entity_validator.bag.logger")
    @patch("gobimport.entity_validator.bag.log_issue")
    @patch("gobimport.entity_validator.bag.Issue")
    def test_validate_panden_invalid_aantal_bouwlagen(self, mock_issue, mock_log_issue, mock_logger):
        self.entities = [
            {
                'identificatie': '03631',
                'aantal_bouwlagen': 10,
                'hoogste_bouwlaag': 10,
                'laagste_bouwlaag': 0
            },
            {
                'identificatie': '03632',
                'aantal_bouwlagen': 11,
                'hoogste_bouwlaag': 10,
                'laagste_bouwlaag': -1
            },
            {
                'identificatie': '03633',
                'aantal_bouwlagen': 12,
                'hoogste_bouwlaag': 13,
                'laagste_bouwlaag': 0
            }
            
        ]
        validator = BAGValidator("bag", "panden", "identificatie")
        for entity in self.entities:
            validator.validate(entity)
        self.assertTrue(validator.result())

        mocked_issue = mock_issue.return_value

        mock_issue.assert_has_calls([
            call(QA_CHECK.Value_aantal_bouwlagen_should_match, self.entities[0], 'identificatie', 'aantal_bouwlagen', compared_to='hoogste_bouwlaag and laagste_bouwlaag', compared_to_value=11),
            call(QA_CHECK.Value_aantal_bouwlagen_should_match, self.entities[1], 'identificatie', 'aantal_bouwlagen', compared_to='hoogste_bouwlaag and laagste_bouwlaag', compared_to_value=12),
            call(QA_CHECK.Value_aantal_bouwlagen_should_match, self.entities[2], 'identificatie', 'aantal_bouwlagen', compared_to='hoogste_bouwlaag and laagste_bouwlaag', compared_to_value=14),
        ])

        mock_log_issue.assert_called_with(mock_logger, 'warning', mocked_issue)

        @patch("gobimport.entity_validator.bag.logger")
        @patch("gobimport.entity_validator.bag.log_issue")
        @patch("gobimport.entity_validator.bag.Issue")
        def test_validate_panden_missing_aantal_bouwlagen(self, mock_issue, mock_log_issue, mock_logger):
            self.entities = [
                {
                    'identificatie': '03631',
                    'aantal_bouwlagen': None,
                    'hoogste_bouwlaag': 10,
                    'laagste_bouwlaag': 0
                },
                {
                    'identificatie': '0427', # Non Amsterdam object will not be validated
                    'aantal_bouwlagen': None,
                    'hoogste_bouwlaag': 10,
                    'laagste_bouwlaag': 0
                }
            ]
            validator = BAGValidator("bag", "panden", "identificatie")
            for entity in self.entities:
                validator.validate(entity)
            self.assertTrue(validator.result())

            mocked_issue = mock_issue.return_value

            mock_issue.assert_has_calls([
                call(QA_CHECK.Value_aantal_bouwlagen_not_filled, self.entities[0], 'identificatie', 'aantal_bouwlagen'),
            ])

            mock_log_issue.assert_called_with(mock_logger, 'warning', mocked_issue)

    @patch("gobimport.entity_validator.bag.log_issue")
    def test_validate_verblijfsobjecten_valid(self, mock_log_issue):
        self.entities = [
            {
                'gebruiksdoel': [{'omschrijving': 'bijeenkomstfunctie'}],
            },
            {
                'gebruiksdoel': [{'omschrijving': 'woonfunctie'}],
                'gebruiksdoel_woonfunctie': {'omschrijving': 'any gebruiksdoel woonfunctie'},
            },
            {
                'gebruiksdoel': [{'omschrijving': 'gezondheidszorgfunctie'}],
                'gebruiksdoel_gezondheidszorgfunctie': {'omschrijving': 'any gebruiksdoel gezondheidszorgfunctie'},
            },
            {
                'gebruiksdoel': [{'omschrijving': 'gezondheidszorgfunctie'}],
                'gebruiksdoel_gezondheidszorgfunctie': {'omschrijving': 'any with complex'},
                'aantal_eenheden_complex': {'omschrijving': 'any value'}
            }
        ]
        validator = BAGValidator("bag", "verblijfsobjecten")
        for entity in self.entities:
            validator.validate(entity)
        self.assertTrue(validator.result())

        self.assertEqual(mock_log_issue.call_count, 0)

    @patch("gobimport.entity_validator.bag.logger")
    @patch("gobimport.entity_validator.bag.log_issue")
    @patch("gobimport.entity_validator.bag.Issue")
    def test_validate_verblijfsobjecten_invalid_gebruiksdoel(self, mock_issue, mock_log_issue, mock_logger):
        self.entities = [
            {
                'identificatie': '03631',
                'gebruiksdoel': [{'omschrijving': 'any invalid gebruiksdoel'}],
            },
            {
                'identificatie': '03632',
                'gebruiksdoel': [{'omschrijving': 'bijeenkomstfunctie'}],
                'gebruiksdoel_woonfunctie': {'omschrijving': 'any woonfunctie'}
            },
            {
                'identificatie': '03633',
                'gebruiksdoel': [{'omschrijving': 'bijeenkomstfunctie'}],
                'gebruiksdoel_gezondheidszorgfunctie': {'omschrijving': 'any gezondheidszorgfunctie'}
            },
            {
                'identificatie': '03634',
                'gebruiksdoel': [{'omschrijving': 'gezondheidszorgfunctie'}],
                'gebruiksdoel_woonfunctie': {'omschrijving': ''},
                'gebruiksdoel_gezondheidszorgfunctie': {'omschrijving': 'any gezondheidszorgfunctie'},
                'aantal_eenheden_complex': 'any value'
            }
        ]
        validator = BAGValidator("bag", "verblijfsobjecten", "identificatie")
        for entity in self.entities:
            validator.validate(entity)
        self.assertTrue(validator.result())

        mocked_issue = mock_issue.return_value

        mock_issue.assert_has_calls([
            call(QA_CHECK.Value_gebruiksdoel_in_domain, self.entities[0], 'identificatie', 'gebruiksdoel'),
            call(QA_CHECK.Value_gebruiksdoel_woonfunctie_should_match, self.entities[1], 'identificatie', 'gebruiksdoel_woonfunctie', compared_to='gebruiksdoel'),
            call(QA_CHECK.Value_gebruiksdoel_gezondheidszorgfunctie_should_match, self.entities[2], 'identificatie', 'gebruiksdoel_gezondheidszorgfunctie', compared_to='gebruiksdoel'),
            call(QA_CHECK.Value_aantal_eenheden_complex_filled, self.entities[3], 'identificatie', 'aantal_eenheden_complex', compared_to='gebruiksdoel_gezondheidszorgfunctie'),
        ])

        mock_log_issue.assert_called_with(mock_logger, 'warning', mocked_issue)

        @patch("gobimport.entity_validator.bag.logger")
        @patch("gobimport.entity_validator.bag.log_issue")
        @patch("gobimport.entity_validator.bag.Issue")
        def test_validate_panden_missing_aantal_bouwlagen(self, mock_issue, mock_log_issue, mock_logger):
            self.entities = [
                {
                    'identificatie': '03631',
                    'aantal_bouwlagen': None,
                    'hoogste_bouwlaag': 10,
                    'laagste_bouwlaag': 0
                }
            ]
            validator = BAGValidator("bag", "panden", "identificatie")
            for entity in self.entities:
                validator.validate(entity)
            self.assertTrue(validator.result())

            mocked_issue = mock_issue.return_value

            mock_issue.assert_has_calls([
                call(QA_CHECK.Value_aantal_bouwlagen_not_filled, self.entities[0], 'identificatie', 'aantal_bouwlagen'),
            ])

            mock_log_issue.assert_called_with(mock_logger, 'warning', mocked_issue)
