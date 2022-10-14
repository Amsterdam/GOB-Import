import datetime
import unittest
from unittest.mock import MagicMock, patch

from gobimport import gob_model
from gobimport.entity_validator import StateValidator


@patch("gobimport.entity_validator.state.logger", MagicMock())
@patch("gobimport.entity_validator.state.log_issue", MagicMock())
@patch("gobimport.entity_validator.gebieden.logger", MagicMock())
@patch("gobimport.entity_validator.gebieden.log_issue", MagicMock())
class TestEntityValidator(unittest.TestCase):

    def setUp(self):
        self.mock_model = MagicMock(spec=gob_model)

        self.entities = []

    @patch('gobimport.entity_validator.state.gob_model.has_states')
    def test_entity_validate_without_state(self, mock_has_states):
        mock_has_states.return_value = False
        result = StateValidator.validates('catalogue', 'collection')
        mock_has_states.assert_called_with('catalogue', 'collection')
        self.assertFalse(result)

    @patch('gobimport.entity_validator.state.gob_model.has_states')
    def test_entity_validate_with_state(self, mock_has_states):
        mock_has_states.return_value = True
        result = StateValidator.validates('catalogue', 'collection')
        mock_has_states.assert_called_with('catalogue', 'collection')
        self.assertTrue(result)

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

    def test_validate_entity_state_volgnummer_none(self):
        self.entities = [
            {
                'identificatie': '1234567890',
                'volgnummer': None,
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

    def test_drop(self):
        self.entities = [
            {
                'identificatie': '1234567890',
                'volgnummer': 1,
                'begin_geldigheid': datetime.datetime(2018, 1, 1),
                'eind_geldigheid': datetime.datetime(2019, 1, 1),
            },
            {
                'identificatie': '1234567890',
                'volgnummer': 2,
                'begin_geldigheid': datetime.datetime(2019, 1, 1),
                'eind_geldigheid': None,
            }
        ]
        validator = StateValidator('catalogue', 'collection', 'identificatie')
        for entity in self.entities:
            validator.validate(entity, drop=True)

        # entity not in cache, assert not dropped
        validator._drop({"identificatie": 18, "volgnummer": 10})
        self.assertNotIn(18, validator.volgnummers)

        for entity in self.entities:
            id_ = entity["identificatie"]
            self.assertIn(id_, validator.volgnummers)
            self.assertIn(entity["volgnummer"], validator.volgnummers[id_])

        self.assertIn(self.entities[1]["identificatie"], validator.end_date)

        # first entity in cache dropped
        validator._drop(self.entities[0])
        id_ = self.entities[0]["identificatie"]
        self.assertIn(id_, validator.volgnummers)
        self.assertNotIn(self.entities[0]["volgnummer"], validator.volgnummers[id_])
        self.assertIn(self.entities[1]["volgnummer"], validator.volgnummers[id_])

        # second entity in cache dropped
        validator._drop(self.entities[1])
        id_ = self.entities[1]["identificatie"]
        self.assertIn(id_, validator.volgnummers)
        self.assertNotIn(self.entities[1]["volgnummer"], validator.volgnummers[id_])
        self.assertFalse(validator.end_date[id_])
