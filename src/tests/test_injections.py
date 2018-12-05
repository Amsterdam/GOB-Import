import unittest
import json

from unittest import mock

from gobimport.injections import inject, _apply_injection

class TestInjections(unittest.TestCase):

    def setUp(self):
        pass

    def test_no_injections(self):
        data = []
        inject(None, data)

        self.assertEqual(data, [])

    @mock.patch('builtins.open')
    def test_empty_injections(self, mock_open):
        injections = []
        mock_open.side_effect = [
            mock.mock_open(read_data=json.dumps(injections)).return_value,
        ]
        inject_spec = {
            "from": "anyfile",
            "on": "id",
            "conversions": {
                "field1": "=",
                "field2": "+"
            }
        }
        data = []
        inject(inject_spec, data)
        self.assertEqual(data, [])

    @mock.patch('builtins.open')
    def test_injections_no_data(self, mock_open):
        injections = [
            {
                "id": 1,
                "field1": "aap",
                "field2": 10
            }
        ]
        mock_open.side_effect = [
            mock.mock_open(read_data=json.dumps(injections)).return_value,
        ]
        inject_spec = {
            "from": "anyfile",
            "on": "id",
            "conversions": {
                "field1": "=",
                "field2": "+"
            }
        }
        data = []
        inject(inject_spec, data)
        self.assertEqual(data, [])

    def test_apply_injection(self):
        row = {}
        _apply_injection(row, "key", "=", "aap")
        self.assertEqual(row, {"key": "aap"})

        row = {"key": 1}
        _apply_injection(row, "key", "+", 1)
        self.assertEqual(row, {"key": 2})

    @mock.patch('builtins.open')
    def test_injections_with_data(self, mock_open):
        injections = [
            {
                "id": 1,
                "field1": "aap",
                "field2": 10
            }
        ]
        mock_open.side_effect = [
            mock.mock_open(read_data=json.dumps(injections)).return_value,
        ]
        inject_spec = {
            "from": "anyfile",
            "on": "id",
            "conversions": {
                "field1": "=",
                "field2": "+"
            }
        }
        data = [
            {
                "id": 0,
                "field1": "0",
                "field2": 0,
                "field3": "x"
            },
            {
                "id": 1,
                "field1": "0",
                "field2": 0,
                "field3": "x"
            },
            {
                "id": 2,
                "field1": "0",
                "field2": 0,
                "field3": "x"
            },
            {
                "id": 1,
                "field1": "0",
                "field2": 0,
                "field3": "x"
            }
        ]
        expect = [
            {
                "id": 0,
                "field1": "0",
                "field2": 0,
                "field3": "x"
            },
            {
                "id": 1,
                "field1": "aap",
                "field2": 10,
                "field3": "x"
            },
            {
                "id": 2,
                "field1": "0",
                "field2": 0,
                "field3": "x"
            },
            {
                "id": 1,
                "field1": "aap",
                "field2": 10,
                "field3": "x"
            }
        ]
        inject(inject_spec, data)
        self.assertEqual(data, expect)


    # def test_validat_data(self):
    #     validator = Validator(self.mock_import_client, 'meetbouten', self.valid_meetbouten, 'identificatie')
    #     validator.validate()
    #
    # def test_duplicate_primary_key(self):
    #     self.valid_meetbouten.append(self.valid_meetbouten[0])
    #     validator = Validator(self.mock_import_client, 'meetbouten', self.valid_meetbouten, 'identificatie')
    #
    #     with self.assertRaises(GOBException):
    #         validator.validate()
    #
    # def test_invalid_data(self):
    #     validator = Validator(self.mock_import_client, 'meetbouten', self.invalid_meetbouten, 'identificatie')
    #     validator.validate()
    #
    #     # Make sure the statusid has been listed as invalid
    #     self.assertEqual(validator.collection_qa['num_invalid_status_id'], 1)
    #
    # def test_fatal_value(self):
    #     validator = Validator(self.mock_import_client, 'meetbouten', self.fatal_meetbouten, 'identificatie')
    #
    #     with self.assertRaises(GOBException):
    #         validator.validate()
    #
    #     # Make sure the identificatie has been listed as invalid
    #     self.assertEqual(validator.collection_qa['num_invalid_identificatie'], 1)
    #
    # def test_missing_warning_data(self):
    #     missing_attr_meetbouten = self.valid_meetbouten[0].pop('status_id')
    #     validator = Validator(self.mock_import_client, 'meetbouten', self.valid_meetbouten, 'identificatie')
    #     validator.validate()
    #
    #     # Make sure the publiceerbaar has been listed as invalid
    #     self.assertEqual(validator.collection_qa['num_invalid_status_id'], 1)
    #
    # def test_missing_fatal_data(self):
    #     missing_attr_meetbouten = self.valid_meetbouten[0].pop('publiceerbaar')
    #     validator = Validator(self.mock_import_client, 'meetbouten', self.valid_meetbouten, 'identificatie')
    #
    #     with self.assertRaises(GOBException):
    #         validator.validate()
    #
    #     # Make sure the publiceerbaar has been listed as invalid
    #     self.assertEqual(validator.collection_qa['num_invalid_publiceerbaar'], 1)
