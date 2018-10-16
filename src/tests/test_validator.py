import unittest
from unittest import mock

from gobcore.exceptions import GOBException

from gobimportclient.import_client import ImportClient
from gobimportclient.validator import Validator

from tests import fixtures


class TestValidator(unittest.TestCase):

    def setUp(self):
        self.valid_meetbouten = fixtures.get_valid_meetbouten()
        self.invalid_meetbouten = fixtures.get_invalid_meetbouten()
        self.fatal_meetbouten = fixtures.get_fatal_meetbouten()

        self.mock_import_client = mock.MagicMock(spec=ImportClient)
        self.mock_import_client.source = 'test'

    def test_validat_data(self):
        validator = Validator(self.mock_import_client, 'meetbouten', self.valid_meetbouten, 'meetboutid')
        validator.validate()

    def test_duplicate_primary_key(self):
        self.valid_meetbouten.append(self.valid_meetbouten[0])
        validator = Validator(self.mock_import_client, 'meetbouten', self.valid_meetbouten, 'meetboutid')

        with self.assertRaises(GOBException):
            validator.validate()

    def test_invalid_data(self):
        validator = Validator(self.mock_import_client, 'meetbouten', self.invalid_meetbouten, 'meetboutid')
        validator.validate()

        # Make sure the statusid has been listed as invalid
        self.assertEqual(validator.collection_qa['num_invalid_status_id'], 1)

    def test_fatal_value(self):
        validator = Validator(self.mock_import_client, 'meetbouten', self.fatal_meetbouten, 'meetboutid')

        with self.assertRaises(GOBException):
            validator.validate()

        # Make sure the meetboutid has been listed as invalid
        self.assertEqual(validator.collection_qa['num_invalid_meetboutid'], 1)

    def test_missing_warning_data(self):
        missing_attr_meetbouten = self.valid_meetbouten[0].pop('status_id')
        validator = Validator(self.mock_import_client, 'meetbouten', self.valid_meetbouten, 'meetboutid')
        validator.validate()

        # Make sure the publiceerbaar has been listed as invalid
        self.assertEqual(validator.collection_qa['num_invalid_status_id'], 1)

    def test_missing_fatal_data(self):
        missing_attr_meetbouten = self.valid_meetbouten[0].pop('publiceerbaar')
        validator = Validator(self.mock_import_client, 'meetbouten', self.valid_meetbouten, 'meetboutid')

        with self.assertRaises(GOBException):
            validator.validate()

        # Make sure the publiceerbaar has been listed as invalid
        self.assertEqual(validator.collection_qa['num_invalid_publiceerbaar'], 1)
