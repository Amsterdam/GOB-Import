import unittest
from unittest.mock import MagicMock, patch

from gobcore.typesystem import GOB
from gobimport.entity_validator.gebieden import _validate_bouwblokken, _validate_buurten


class TestEntityValidator(unittest.TestCase):

    def setUp(self):
        self.entities = []
        self.log = MagicMock()
        pass

    def test_validate_bouwblokken_valid(self):
        self.entities = [
            {
                'identificatie': GOB.String.from_value('1234567890'),
                'begin_geldigheid': GOB.Date.from_value('2018-01-01'),
                'eind_geldigheid': GOB.Date.from_value(None),
            }
        ]
        self.assertTrue(_validate_bouwblokken(self.entities, self.log))

    def test_validate_bouwblokken_invalid(self):
        self.entities = [
            {
                'identificatie': GOB.String.from_value('1234567890'),
                'begin_geldigheid': GOB.Date.from_value('2020-01-01'),
                'eind_geldigheid': GOB.Date.from_value(None),
            }
        ]
        self.assertFalse(_validate_bouwblokken(self.entities, self.log))

    def test_validate_buurten_valid(self):
        self.entities = [
            {
                'identificatie': GOB.String.from_value('1234567890'),
                'documentdatum': GOB.Date.from_value('2018-01-01'),
                'eind_geldigheid': GOB.Date.from_value('2019-01-01'),
            }
        ]
        self.assertTrue(_validate_buurten(self.entities, self.log))

    def test_validate_buurten_invalid(self):
        self.entities = [
            {
                'identificatie': GOB.String.from_value('1234567890'),
                'documentdatum': GOB.Date.from_value('2020-01-01'),
                'eind_geldigheid': GOB.Date.from_value('2019-01-01'),
            }
        ]

        # This test should only call log with a warning statement and return True
        self.assertTrue(_validate_buurten(self.entities, self.log))
        self.log.assert_called_with(
            "warning",
            "documentdatum can not be after eind_geldigheid",
            {'data': {
                'identificatie': self.entities[0]['identificatie'],
                'documentdatum': self.entities[0]['documentdatum'],
                'eind_geldigheid': self.entities[0]['eind_geldigheid'],
            }}
        )
