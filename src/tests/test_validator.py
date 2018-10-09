import unittest

from gobcore.exceptions import GOBException

from gobimportclient.validator import validate

from tests import fixtures

class TestValidator(unittest.TestCase):

    def setUp(self):
        self.valid_meetbouten = fixtures.get_valid_meetbouten()
        self.invalid_meetbouten = fixtures.get_invalid_meetbouten()

    def test_validate(self):
        # Validate correct data
        validate('meetbouten', self.valid_meetbouten, 'meetboutid')

        # Validate invalid data, expect an AssertionError to be raised
        with self.assertRaises(AssertionError):
            validate('meetbouten', self.invalid_meetbouten, 'meetboutid')

        # Test that missing entity validator doesn't fail
        validate('test', [], 'testid')

        # Test that duplicate primary fails
        with self.assertRaises(GOBException):
            self.valid_meetbouten.append(self.valid_meetbouten[0])
            validate('meetbouten', self.valid_meetbouten, 'meetboutid')
