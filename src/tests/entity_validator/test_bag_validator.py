import datetime
import unittest
from unittest.mock import MagicMock, patch, call

from gobcore.typesystem import GOB
from gobimport.entity_validator.bag import BAGValidator
from gobcore.quality.issue import QA_CHECK


class TestEntityValidator(unittest.TestCase):

    def setUp(self):
        self.entities = []

    def test_validates(self):
        self.assertTrue(BAGValidator.validates('bag', 'panden'))
        self.assertFalse(BAGValidator.validates('any catalog', 'any collection'))

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
            call(QA_CHECK.Value_aantal_bouwlagen_should_match, self.entities[0], 'identificatie', 'aantal_bouwlagen', compared_to='hoogste_bouwlaag and laagste_bouwlaag combined', compared_to_value=11),
            call(QA_CHECK.Value_aantal_bouwlagen_should_match, self.entities[1], 'identificatie', 'aantal_bouwlagen', compared_to='hoogste_bouwlaag and laagste_bouwlaag combined', compared_to_value=12),
            call(QA_CHECK.Value_aantal_bouwlagen_should_match, self.entities[2], 'identificatie', 'aantal_bouwlagen', compared_to='hoogste_bouwlaag and laagste_bouwlaag combined', compared_to_value=14),
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
    @patch("gobimport.entity_validator.bag.BAGValidator._check_gebruiksdoel_plus", MagicMock())
    @patch("gobimport.entity_validator.bag.BAGValidator._check_aantal_eenheden_complex", MagicMock())
    def test_validate_verblijfsobjecten_valid(self, mock_log_issue):
        self.entities = [
            {
                'gebruiksdoel': [{'omschrijving': 'woonfunctie'}],
            },
            {
                'gebruiksdoel': [{'omschrijving': 'woonfunctie'},
                                 {'omschrijving': 'bijeenkomstfunctie'},
                                 {'omschrijving': 'celfunctie'},
                                 {'omschrijving': 'gezondheidszorgfunctie'},
                                 {'omschrijving': 'industriefunctie'},
                                 {'omschrijving': 'kantoorfunctie'},
                                 {'omschrijving': 'logiesfunctie'},
                                 {'omschrijving': 'onderwijsfunctie'},
                                 {'omschrijving': 'sportfunctie'},
                                 {'omschrijving': 'winkelfunctie'},
                                 {'omschrijving': 'overige gebruiksfunctie'}],
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
    @patch("gobimport.entity_validator.bag.BAGValidator._check_gebruiksdoel_plus", MagicMock())
    @patch("gobimport.entity_validator.bag.BAGValidator._check_aantal_eenheden_complex", MagicMock())
    def test_validate_verblijfsobjecten_invalid_gebruiksdoel(self, mock_issue, mock_log_issue, mock_logger):
        self.entity = {
            'identificatie': '03631',
            'gebruiksdoel': [{'omschrijving': 'any invalid gebruiksdoel'}],
        }

        validator = BAGValidator("bag", "verblijfsobjecten", "identificatie")
        validator.validate(self.entity)
        self.assertTrue(validator.result())

        mocked_issue = mock_issue.return_value
        mock_issue.assert_called_with(QA_CHECK.Value_gebruiksdoel_in_domain, self.entity, 'identificatie', 'gebruiksdoel')
        mock_log_issue.assert_called_with(mock_logger, 'warning', mocked_issue)

    @patch("gobimport.entity_validator.bag.logger")
    @patch("gobimport.entity_validator.bag.log_issue")
    @patch("gobimport.entity_validator.bag.Issue")
    def test_validate_standplaats_ligplaats(self, mock_issue, mock_log_issue, mock_logger):
        entity = {
            'identificatie': '03631',
            'gebruiksdoel': [{
                'omschrijving': 'woonfunctie',
            }, {
                'omschrijving': 'sportfunctie',
            }]
        }

        # No issues ligplaatsen
        validator = BAGValidator("bag", "ligplaatsen", "identificatie")
        validator.validate(entity)
        self.assertTrue(validator.result())
        mock_issue.assert_not_called()
        mock_log_issue.assert_not_called()

        # No issues standplaatsen
        validator = BAGValidator("bag", "standplaatsen", "identificatie")
        validator.validate(entity)
        self.assertTrue(validator.result())
        mock_issue.assert_not_called()
        mock_log_issue.assert_not_called()

        entity = {
            'identificatie': '03631',
            'gebruiksdoel': [{
                'omschrijving': 'invalid'
            }, {
                'omschrijving': 'woonfunctie',
            }, {
                'omschrijving': 'woonfunctie',
            }]
        }

        # With issues ligplaatsen
        validator = BAGValidator("bag", "ligplaatsen", "identificatie")
        validator.validate(entity)
        self.assertTrue(validator.result())

        mock_issue.assert_has_calls([
            call(QA_CHECK.Value_gebruiksdoel_in_domain, entity, 'identificatie', 'gebruiksdoel'),
            call(QA_CHECK.Value_duplicates, entity, 'identificatie', 'gebruiksdoel'),
        ])

        mock_log_issue.assert_has_calls([
            call(mock_logger, 'warning', mock_issue.return_value),
            call(mock_logger, 'warning', mock_issue.return_value),
        ])

        mock_issue.reset_mock()
        mock_log_issue.reset_mock()

        # With issues standplaatsen
        validator = BAGValidator("bag", "standplaatsen", "identificatie")
        validator.validate(entity)
        self.assertTrue(validator.result())

        mock_issue.assert_has_calls([
            call(QA_CHECK.Value_gebruiksdoel_in_domain, entity, 'identificatie', 'gebruiksdoel'),
            call(QA_CHECK.Value_duplicates, entity, 'identificatie', 'gebruiksdoel'),
        ])

        mock_log_issue.assert_has_calls([
            call(mock_logger, 'warning', mock_issue.return_value),
            call(mock_logger, 'warning', mock_issue.return_value),
        ])


    @patch("gobimport.entity_validator.bag.logger")
    @patch("gobimport.entity_validator.bag.log_issue")
    @patch("gobimport.entity_validator.bag.Issue")
    def test_check_gebruiksdoel_plus(self, mock_issue, mock_log_issue, mock_logger):
        self.valid_entities = [
            {
                'identificatie': '03631',
                'gebruiksdoel': [{'omschrijving': 'woonfunctie'}],
                'gebruiksdoel_woonfunctie': {'omschrijving': 'any woonfunctie'},
            },
            {
                'identificatie': '03632',
                'gebruiksdoel': [{'omschrijving': 'gezondheidszorgfunctie'}],
                'gebruiksdoel_gezondheidszorgfunctie': {'omschrijving': 'any gezondheidszorgfunctie'},
            }]

        self.invalid_entities = [
            {
                'identificatie': '03631',
                'gebruiksdoel': [{'omschrijving': 'other gebruiksdoel'}],
                'gebruiksdoel_woonfunctie': {'omschrijving': 'any woonfunctie'},
            },
            {
                'identificatie': '03632',
                'gebruiksdoel': [{'omschrijving': 'other gebruiksdoel'}],
                'gebruiksdoel_gezondheidszorgfunctie': {'omschrijving': 'any gezondheidszorgfunctie'},
            }]

        validator = BAGValidator("bag", "verblijfsobjecten", "identificatie")
        for entity in self.valid_entities:
            gebruiksdoelen = [gebruiksdoel.get('omschrijving') for gebruiksdoel in entity.get('gebruiksdoel')]
            validator._check_gebruiksdoel_plus(entity, gebruiksdoelen)

        self.assertEqual(mock_log_issue.call_count, 0)

        for entity in self.invalid_entities:
            gebruiksdoelen = [gebruiksdoel.get('omschrijving') for gebruiksdoel in entity.get('gebruiksdoel')]
            validator._check_gebruiksdoel_plus(entity, gebruiksdoelen)

        mocked_issue = mock_issue.return_value
        mock_issue.assert_has_calls([
            call(QA_CHECK.Value_gebruiksdoel_woonfunctie_should_match, self.invalid_entities[0], 'identificatie', 'gebruiksdoel_woonfunctie', compared_to='gebruiksdoel'),
            call(QA_CHECK.Value_gebruiksdoel_gezondheidszorgfunctie_should_match, self.invalid_entities[1], 'identificatie', 'gebruiksdoel_gezondheidszorgfunctie', compared_to='gebruiksdoel'),
        ])
        mock_log_issue.assert_called_with(mock_logger, 'warning', mocked_issue)

    @patch("gobimport.entity_validator.bag.logger")
    @patch("gobimport.entity_validator.bag.log_issue")
    @patch("gobimport.entity_validator.bag.Issue")
    def test_check_aantal_eenheden_complex(self, mock_issue, mock_log_issue, mock_logger):
        self.valid_entities = [
            {
                'identificatie': '03631',
                'aantal_eenheden_complex': 10,
                'gebruiksdoel_woonfunctie': {'omschrijving': 'any complex'},
            },
            {
                'identificatie': '03632',
                'aantal_eenheden_complex': 10,
                'gebruiksdoel_gezondheidszorgfunctie': {'omschrijving': 'any Complex'},
            }]

        self.invalid_entities = [
            {
                'identificatie': '03631',
                'aantal_eenheden_complex': 10,
                'gebruiksdoel_woonfunctie': {'omschrijving': 'any woonfunctie'},
                'gebruiksdoel_gezondheidszorgfunctie': {'omschrijving': 'any gezondheidszorgfunctie'},
            },
            {
                'identificatie': '03632',
                'aantal_eenheden_complex': None,
                'gebruiksdoel_woonfunctie': {'omschrijving': 'any complex'},
            },
            {
                'identificatie': '03633',
                'aantal_eenheden_complex': None,
                'gebruiksdoel_gezondheidszorgfunctie': {'omschrijving': 'any Complex'},
            }]

        validator = BAGValidator("bag", "verblijfsobjecten", "identificatie")
        for entity in self.valid_entities:
            validator._check_aantal_eenheden_complex(entity)

        self.assertEqual(mock_log_issue.call_count, 0)

        for entity in self.invalid_entities:
            validator._check_aantal_eenheden_complex(entity)

        mocked_issue = mock_issue.return_value
        mock_issue.assert_has_calls([
            call(QA_CHECK.Value_aantal_eenheden_complex_should_be_empty, self.invalid_entities[0], 'identificatie', 'aantal_eenheden_complex', compared_to='gebruiksdoel_woonfunctie and gebruiksdoel_gezondheidszorgfunctie', compared_to_value='any woonfunctie, any gezondheidszorgfunctie'),
            call(QA_CHECK.Value_aantal_eenheden_complex_should_be_filled, self.invalid_entities[1], 'identificatie', 'aantal_eenheden_complex', compared_to='gebruiksdoel_woonfunctie and gebruiksdoel_gezondheidszorgfunctie', compared_to_value='any complex, '),
            call(QA_CHECK.Value_aantal_eenheden_complex_should_be_filled, self.invalid_entities[2], 'identificatie', 'aantal_eenheden_complex', compared_to='gebruiksdoel_woonfunctie and gebruiksdoel_gezondheidszorgfunctie', compared_to_value=', any Complex'),
        ])
        mock_log_issue.assert_called_with(mock_logger, 'warning', mocked_issue)