import datetime
import unittest
from unittest.mock import MagicMock, patch

from gobcore.typesystem import GOB
from gobimport.entity_validator.gebieden import GebiedenValidator


class TestEntityValidator(unittest.TestCase):

    def setUp(self):
        self.entities = []

    def test_validate_bouwblokken_valid(self):
        self.entities = [
            {
                'identificatie': '1234567890',
                'begin_geldigheid': datetime.date(2018, 1, 1),
                'eind_geldigheid': None,
            }
        ]
        validator = GebiedenValidator("gebieden", "bouwblokken", "identificatie")
        for entity in self.entities:
            validator.validate(entity)
        self.assertTrue(validator.result())

    @patch("gobimport.entity_validator.gebieden.logger", MagicMock())
    @patch("gobimport.entity_validator.gebieden.log_issue", MagicMock())
    def test_validate_bouwblokken_invalid(self):
        self.entities = [
            {
                'identificatie': '1234567890',
                'begin_geldigheid': datetime.date(2020, 1, 1),
                'eind_geldigheid': None,
            }
        ]
        validator = GebiedenValidator("gebieden", "bouwblokken", "identificatie")
        for entity in self.entities:
            validator.validate(entity)
        self.assertTrue(validator.result())

    @patch("gobimport.entity_validator.gebieden.log_issue")
    def test_validate_buurten_valid(self, mock_logger):
        mock_logger.warning = MagicMock()
        self.entities = [
            {
                'identificatie': '1234567890',
                'documentdatum': datetime.date(2018, 1, 1),
                'eind_geldigheid': datetime.date(2019, 1, 1),
                'registratiedatum': datetime.datetime(2020, 1, 1),
            }
        ]
        # This test should only call log with a warning statement and return True
        validator = GebiedenValidator("gebieden", "buurten", "identificatie")
        for entity in self.entities:
            validator.validate(entity)
        self.assertTrue(validator.result())
        mock_logger.assert_called()

    @patch("gobimport.entity_validator.gebieden.log_issue")
    def test_validate_buurten_invalid(self, mock_logger):
        mock_logger.warning = MagicMock()
        self.entities = [
            {
                'identificatie': '1234567890',
                'documentdatum': datetime.date(2020, 1, 1),
                'eind_geldigheid': datetime.date(2019, 1, 1),
                'registratiedatum': datetime.datetime(2019, 1, 1),
            }
        ]

        # This test should only call log with a warning statement and return True
        validator = GebiedenValidator("gebieden", "buurten", "identificatie")
        for entity in self.entities:
            validator.validate(entity)
        self.assertTrue(validator.result())
        mock_logger.assert_called()
