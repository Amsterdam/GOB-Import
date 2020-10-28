import datetime
import unittest
from unittest.mock import MagicMock, patch

from gobcore.exceptions import GOBException
from gobcore.model import GOBModel
from gobcore.typesystem import GOB
from gobimport.entity_validator import StateValidator


@patch("gobimport.entity_validator.state.logger", MagicMock())
@patch("gobimport.entity_validator.state.log_issue", MagicMock())
@patch("gobimport.entity_validator.gebieden.logger", MagicMock())
@patch("gobimport.entity_validator.gebieden.log_issue", MagicMock())
class TestEntityValidator(unittest.TestCase):

    def setUp(self):
        self.mock_model = MagicMock(spec=GOBModel)

        self.entities = []

    @patch('gobimport.entity_validator.state.GOBModel')
    def test_entity_validate_without_state(self, mock_model):
        mock_model.return_value = self.mock_model
        self.mock_model.has_states.return_value = False
        self.assertFalse(StateValidator.validates('catalogue', 'collection'))

    @patch('gobimport.entity_validator.state.GOBModel')
    def test_entity_validate_with_state(self, mock_model):
        mock_model.return_value = self.mock_model
        self.mock_model.has_states.return_value = True
        self.assertTrue(StateValidator.validates('catalogue', 'collection'))

    def test_validate_entity_state_valid(self):
        self.entities = [
            {
                'identificatie': '1234567890',
                'volgnummer': 1,
                'begin_geldigheid': datetime.datetime(2018, 1, 1),
                'eind_geldigheid': datetime.datetime(2019, 1, 1),
            }
        ]
        validator = StateValidator('catalogue', 'collection', 'identificatie')
        for entity in self.entities:
            validator.validate(entity)
        self.assertTrue(validator.result())

    def test_validate_entity_state_invalid_begin_geldigheid(self):
        self.entities = [
            {
                'identificatie': '1234567890',
                'volgnummer': 1,
                'begin_geldigheid': datetime.datetime(2019, 2, 1),
                'eind_geldigheid': datetime.datetime(2019, 1, 1),
            }
        ]

        validator = StateValidator('catalogue', 'collection', 'identificatie')
        for entity in self.entities:
            validator.validate(entity)
        self.assertTrue(validator.result())

    def test_validate_entity_state_missing_begin_geldigheid(self):
        self.entities = [
            {
                'identificatie': '1234567890',
                'volgnummer': 1,
                'begin_geldigheid': None,
                'eind_geldigheid': datetime.datetime(2019, 1, 1),
            }
        ]

        validator = StateValidator('catalogue', 'collection', 'identificatie')
        for entity in self.entities:
            validator.validate(entity)
        self.assertFalse(validator.result())

    def test_validate_entity_state_invalid_volgnummer(self):
        self.entities = [
            {
                'identificatie': '1234567890',
                'volgnummer': -1,
                'begin_geldigheid': datetime.datetime(2018, 1, 1),
                'eind_geldigheid': datetime.datetime(2019, 1, 1),
            }
        ]

        validator = StateValidator('catalogue', 'collection', 'identificatie')
        for entity in self.entities:
            validator.validate(entity)
        self.assertFalse(validator.result())

    def test_validate_entity_state_duplicate_volgnummer(self):
        self.entities = [
            {
                'identificatie': '1234567890',
                'volgnummer': 1,
                'begin_geldigheid': datetime.datetime(2018, 1, 1),
                'eind_geldigheid': datetime.datetime(2019, 1, 1),
            },
            {
                'identificatie': '1234567890',
                'volgnummer': 1,
                'begin_geldigheid': datetime.datetime(2018, 1, 1),
                'eind_geldigheid': datetime.datetime(2019, 1, 1),
            }
        ]

        validator = StateValidator('catalogue', 'collection', 'identificatie')
        for entity in self.entities:
            validator.validate(entity)
        self.assertFalse(validator.result())

    def test_validate_entity_state_multiple_open_end_dates(self):
        self.entities = [
            {
                'identificatie': '1234567890',
                'volgnummer': 1,
                'begin_geldigheid': datetime.datetime(2018, 1, 1),
                'eind_geldigheid': None,
            },
            {
                'identificatie': '1234567890',
                'volgnummer': 2,
                'begin_geldigheid': datetime.datetime(2018, 1, 1),
                'eind_geldigheid': None,
            }
        ]

        with patch("gobimport.entity_validator.state.log_issue") as mock_log_issue:
            validator = StateValidator('catalogue', 'collection', 'identificatie')
            for entity in self.entities:
                validator.validate(entity)
            self.assertTrue(validator.result())

            # Expect Log Issue to be called
            mock_log_issue.assert_called()