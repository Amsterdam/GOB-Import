import unittest
from unittest.mock import MagicMock, patch

from gobcore.exceptions import GOBException
from gobimport.entity_validator import EntityValidator, StateValidator, GebiedenValidator


@patch("gobimport.entity_validator.state.logger", MagicMock())
@patch("gobimport.entity_validator.gebieden.logger", MagicMock())
class TestEntityValidator(unittest.TestCase):

    def test_entity_result_ok(self):
        with patch.object(StateValidator, 'validates', lambda *args: True), \
             patch.object(GebiedenValidator, 'validate', lambda *args, **kwargs: True):
            validator = EntityValidator("catalog", "collection", "id")
            self.assertTrue(validator.result())

    def test_entity_result_error(self):
        with patch.object(StateValidator, 'validates', lambda *args: True), \
             patch.object(GebiedenValidator, 'validates', lambda *args: True), \
             patch.object(StateValidator, 'result', lambda *args: True), \
             patch.object(GebiedenValidator, 'result', lambda *args: False), \
             self.assertRaises(GOBException):
            validator = EntityValidator("catalog", "collection", "id")
            validator.result()

        with patch.object(StateValidator, 'validates', lambda *args: True), \
             patch.object(GebiedenValidator, 'validates', lambda *args: True), \
             patch.object(StateValidator, 'result', lambda *args: False), \
             patch.object(GebiedenValidator, 'result', lambda *args: True), \
             self.assertRaises(GOBException):
            validator = EntityValidator("catalog", "collection", "id")
            validator.result()

    def test_entity_validate(self):
        with patch.object(StateValidator, 'validates', lambda *args: True), \
             patch.object(StateValidator, 'validate', lambda *args, **kwargs: True), \
             patch.object(GebiedenValidator, 'validates', lambda *args: True), \
             patch.object(GebiedenValidator, 'validate', lambda *args, **kwargs: False):
            validator = EntityValidator("catalog", "collection", "id")
            self.assertEqual(len(validator.validators), 2)
            self.assertIsNone(validator.validate("collection"))

    @patch("gobimport.entity_validator.StateValidator", MagicMock())
    @patch("gobimport.entity_validator.GebiedenValidator", MagicMock())
    def test_validate_kwarg(self):
        validator = EntityValidator("catalog", "collection", "id")
        validator.validate("collection", custom_kwarg="kwarg1")

        self.assertEquals(2, len(validator.validators))
        for val in validator.validators:
            val.validate.assert_called_with("collection", custom_kwarg="kwarg1")
