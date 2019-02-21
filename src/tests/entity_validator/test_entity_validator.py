import unittest
from unittest.mock import MagicMock, patch

from gobcore.exceptions import GOBException
from gobcore.model import GOBModel
from gobcore.typesystem import GOB
from gobimport.entity_validator import entity_validate, _validate_entity_state


@patch("gobimport.entity_validator.logger", MagicMock())
class TestEntityValidator(unittest.TestCase):

    def setUp(self):
        self.mock_model = MagicMock(spec=GOBModel)

        self.entities = []

    @patch('gobimport.entity_validator.GOBModel')
    @patch('gobimport.entity_validator._validate_entity_state')
    def test_entity_validate_without_state(self, mock_validate_entity_state, mock_model):
        mock_model.return_value = self.mock_model
        self.mock_model.has_states.return_value = False
        entity_validate('catalogue', 'collection', self.entities, "identificatie")

        # Assert _validate_entity_state not called for collections without state
        mock_validate_entity_state.assert_not_called()

    @patch('gobimport.entity_validator.GOBModel')
    @patch('gobimport.entity_validator._validate_entity_state')
    def test_entity_validate_with_state(self, mock_validate_entity_state, mock_model):
        mock_model.return_value = self.mock_model
        self.mock_model.has_states.return_value = True
        entity_validate('catalogue', 'collection', self.entities, "identificatie")

        # Assert _validate_entity_state called for collections with state
        mock_validate_entity_state.assert_called_once()

    @patch('gobimport.entity_validator.GOBModel')
    @patch('gobimport.entity_validator._validate_entity_state')
    def test_entity_validate_with_state_invalid(self, mock_validate_entity_state, mock_model):
        mock_model.return_value = self.mock_model
        self.mock_model.has_states.return_value = True
        mock_validate_entity_state.return_value = False

        # Assert a GOBException was raised when _validate_entity_state fails
        with self.assertRaises(GOBException):
            entity_validate('catalogue', 'buurten', self.entities, "identificatie")

    def test_validate_entity_state_valid(self):
        self.entities = [
            {
                'identificatie': GOB.String.from_value('1234567890'),
                'volgnummer': GOB.String.from_value('1'),
                'begin_geldigheid': GOB.Date.from_value('2018-01-01'),
                'eind_geldigheid': GOB.Date.from_value('2019-01-01'),
            }
        ]

        self.assertTrue(_validate_entity_state(self.entities, "identificatie"))

    def test_validate_entity_state_invalid_begin_geldigheid(self):
        self.entities = [
            {
                'identificatie': GOB.String.from_value('1234567890'),
                'volgnummer': GOB.String.from_value('1'),
                'begin_geldigheid': GOB.Date.from_value('2019-02-01'),
                'eind_geldigheid': GOB.Date.from_value('2019-01-01'),
            }
        ]

        # invalid begin geldigheid is not an error but a warning
        self.assertTrue(_validate_entity_state(self.entities, "identificatie"))

    def test_validate_entity_state_invalid_volgnummer(self):
        self.entities = [
            {
                'identificatie': GOB.String.from_value('1234567890'),
                'volgnummer': GOB.String.from_value('-1'),
                'begin_geldigheid': GOB.Date.from_value('2018-01-01'),
                'eind_geldigheid': GOB.Date.from_value('2019-01-01'),
            }
        ]

        # Assert false returned and log to be called
        self.assertFalse(_validate_entity_state(self.entities, "identificatie"))

    def test_validate_entity_state_duplicate_volgnummer(self):
        self.entities = [
            {
                'identificatie': GOB.String.from_value('1234567890'),
                'volgnummer': GOB.String.from_value('1'),
                'begin_geldigheid': GOB.Date.from_value('2018-01-01'),
                'eind_geldigheid': GOB.Date.from_value('2019-01-01'),
            },
            {
                'identificatie': GOB.String.from_value('1234567890'),
                'volgnummer': GOB.String.from_value('1'),
                'begin_geldigheid': GOB.Date.from_value('2018-01-01'),
                'eind_geldigheid': GOB.Date.from_value('2019-01-01'),
            }
        ]

        # Assert false returned and log to be called
        self.assertFalse(_validate_entity_state(self.entities, "identificatie"))
