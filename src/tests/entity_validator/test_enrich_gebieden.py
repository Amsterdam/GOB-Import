import unittest
from unittest.mock import MagicMock, patch

from gobcore.typesystem import GOB
from gobimport.entity_validator.gebieden import _validate_bouwblokken, _validate_buurten


class TestEntityValidator(unittest.TestCase):

    def setUp(self):
        self.entities = []

    def test_validate_bouwblokken_valid(self):
        self.entities = [
            {
                'identificatie': GOB.String.from_value('1234567890'),
                'begin_geldigheid': GOB.Date.from_value('2018-01-01'),
                'eind_geldigheid': GOB.Date.from_value(None),
            }
        ]
        self.assertTrue(_validate_bouwblokken(self.entities, "identificatie"))

    @patch("gobimport.entity_validator.gebieden.logger", MagicMock())
    def test_validate_bouwblokken_invalid(self):
        self.entities = [
            {
                'identificatie': GOB.String.from_value('1234567890'),
                'begin_geldigheid': GOB.Date.from_value('2020-01-01'),
                'eind_geldigheid': GOB.Date.from_value(None),
            }
        ]
        self.assertFalse(_validate_bouwblokken(self.entities, "identificatie"))

    def test_validate_buurten_valid(self):
        self.entities = [
            {
                'identificatie': GOB.String.from_value('1234567890'),
                'documentdatum': GOB.Date.from_value('2018-01-01'),
                'eind_geldigheid': GOB.Date.from_value('2019-01-01'),
                'registratiedatum': GOB.Date.from_value('2019-01-01'),
            }
        ]
        self.assertTrue(_validate_buurten(self.entities, "identificatie"))

    @patch("gobimport.entity_validator.gebieden.logger")
    def test_validate_buurten_invalid(self, mock_logger):
        mock_logger.warning = MagicMock()
        self.entities = [
            {
                'identificatie': GOB.String.from_value('1234567890'),
                'documentdatum': GOB.Date.from_value('2020-01-01'),
                'eind_geldigheid': GOB.Date.from_value('2019-01-01'),
                'registratiedatum': GOB.Date.from_value('2019-01-01'),
            }
        ]

        # This test should only call log with a warning statement and return True
        self.assertTrue(_validate_buurten(self.entities, "identificatie"))
        mock_logger.warning.assert_called()
