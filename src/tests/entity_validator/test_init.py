import datetime
import unittest
from unittest.mock import MagicMock, patch

from gobcore.exceptions import GOBException
from gobcore.model import GOBModel
from gobcore.typesystem import GOB

from gobimport.entity_validator import EntityValidator, StateValidator, GebiedenValidator


@patch("gobimport.entity_validator.state.logger", MagicMock())
@patch("gobimport.entity_validator.gebieden.logger", MagicMock())
class TestEntityValidator(unittest.TestCase):

    def setUp(self):
        pass

    def test_entity_validate_ok(self):
        with patch.object(StateValidator, 'validates', lambda *args: True),\
            patch.object(GebiedenValidator, 'validate', lambda *args: True):
            validator = EntityValidator("catalog", "collection", "id")
            self.assertTrue(validator.result())

    def test_entity_validate_error(self):
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
