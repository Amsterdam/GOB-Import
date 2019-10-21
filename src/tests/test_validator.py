import unittest
from unittest import mock

from gobcore.exceptions import GOBException
from gobcore.logging.logger import Logger

from gobimport.import_client import ImportClient
from gobimport.validator import Validator

from tests import fixtures


@mock.patch("gobcore.logging.logger.logger", mock.MagicMock(spec=Logger))
@mock.patch("gobcore.logging.logger.logger.info", mock.MagicMock())
@mock.patch("gobcore.logging.logger.logger.warning", mock.MagicMock())
@mock.patch("gobcore.logging.logger.logger.error", mock.MagicMock())
class TestValidator(unittest.TestCase):

    def setUp(self):
        self.valid_meetbouten = fixtures.get_valid_meetbouten()
        self.invalid_meetbouten = fixtures.get_invalid_meetbouten()
        self.fatal_meetbouten = fixtures.get_fatal_meetbouten()
        self.nopubliceerbaar_meetbouten = fixtures.get_nopubliceerbaar_meetbouten()
        self.nullpubliceerbaar_meetbouten = fixtures.get_nullpubliceerbaar_meetbouten()

        self.mock_input_spec = {
            'catalogue': 'meetbouten',
            'entity': 'meetbouten',
            'source': {
                'entity_id': 'identificatie'
            }
        }

        self.mock_import_client = mock.MagicMock(spec=ImportClient)
        self.mock_import_client.source = {
            'name': 'test'
        }

    def test_validat_data(self):
        validator = Validator('source_app', 'meetbouten', 'identificatie', self.mock_input_spec)
        for entity in self.valid_meetbouten:
            validator.validate(entity)

    def test_duplicate_primary_key(self):
        self.valid_meetbouten.append(self.valid_meetbouten[0])
        validator = Validator('source_app', 'meetbouten', 'identificatie', self.mock_input_spec)

        with self.assertRaises(GOBException):
            for entity in self.valid_meetbouten:
                validator.validate(entity)
            validator.result()

    def test_valid_primary_key_with_states(self):
        mock_input_spec = {
            'catalogue': 'bag',
            'entity': 'woonplaatsen',
            'source': {
                'entity_id': 'identificatie'
            }
        }

        valid_entity = fixtures.get_valid_entity_with_state()
        validator = Validator('source_app', 'woonplaatsen', 'identificatie', mock_input_spec)

        for entity in valid_entity:
            validator.validate(entity)
        validator.result()

    def test_valid_primary_key_with_states_other_seqnr(self):
        mock_input_spec = {
            'catalogue': 'bag',
            'entity': 'woonplaatsen',
            'source': {
                'entity_id': 'identificatie'
            },
            'gob_mapping': {
                'volgnummer': {
                    'source_mapping': 'nummervolg'
                }
            }
        }

        valid_entity = [{'identificatie': '1234', 'nummervolg': '1'}]
        validator = Validator('source_app', 'woonplaatsen', 'identificatie', mock_input_spec)

        for entity in valid_entity:
            validator.validate(entity)
        validator.result()

    def test_duplicate_primary_key_with_states_other_seqnr(self):
        mock_input_spec = {
            'catalogue': 'bag',
            'entity': 'woonplaatsen',
            'source': {
                'entity_id': 'identificatie'
            },
            'gob_mapping': {
                'volgnummer': {
                    'source_mapping': 'nummervolg'
                }
            }
        }

        valid_entity = [{'identificatie': '1234', 'nummervolg': '1'}]
        valid_entity.append(valid_entity[0])
        validator = Validator('source_app', 'woonplaatsen', 'identificatie', mock_input_spec)

        with self.assertRaises(GOBException):
            for entity in valid_entity:
                validator.validate(entity)
            validator.result()

    def test_invalid_data(self):
        validator = Validator('source_app', 'meetbouten', 'identificatie', self.mock_input_spec)
        for entity in self.invalid_meetbouten:
            validator.validate(entity)

        # Make sure the statusid has been listed as invalid
        self.assertEqual(validator.collection_qa['num_invalid_status_id'], 1)

    def test_fatal_value(self):
        validator = Validator('source_app', 'meetbouten', 'identificatie', self.mock_input_spec)

        with self.assertRaises(GOBException):
            for entity in self.fatal_meetbouten:
                validator.validate(entity)
            validator.result()

        # Make sure the identificatie has been listed as invalid
        self.assertEqual(validator.collection_qa['num_invalid_identificatie'], 1)

    def test_missing_warning_data(self):
        missing_attr_meetbouten = self.valid_meetbouten[0].pop('status_id')
        validator = Validator('source_app', 'meetbouten', 'identificatie', self.mock_input_spec)

        for entity in self.valid_meetbouten:
            validator.validate(entity)

        # Make sure the publiceerbaar has been listed as invalid
        self.assertEqual(validator.collection_qa['num_invalid_status_id'], 1)

    def test_missing_fatal_data(self):
        missing_attr_meetbouten = self.valid_meetbouten[0].pop('publiceerbaar')
        validator = Validator('source_app', 'meetbouten', 'identificatie', self.mock_input_spec)

        for entity in self.valid_meetbouten:
            validator.validate(entity)

        # Make sure the publiceerbaar has been listed as invalid
        self.assertEqual(validator.collection_qa['num_invalid_publiceerbaar'], 1)   # Warning

    def test_nopubliceerbaar(self):
        validator = Validator('source_app', 'meetbouten', 'identificatie', self.mock_input_spec)

        for entity in self.nopubliceerbaar_meetbouten:
            validator.validate(entity)

        # Make sure the publiceerbaar has been listed as invalid
        self.assertEqual(validator.collection_qa['num_invalid_publiceerbaar'], 1)   # Warning

    def test_nullpubliceerbaar(self):
        validator = Validator('source_app', 'meetbouten', 'identificatie', self.mock_input_spec)

        for entity in self.nullpubliceerbaar_meetbouten:
            validator.validate(entity)

        # Make sure the publiceerbaar has been listed as invalid
        self.assertEqual(validator.collection_qa['num_invalid_publiceerbaar'], 0)
