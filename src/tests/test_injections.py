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
                "field2": 2,
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
                "field2": 12,
                "field3": "x"
            }
        ]
        inject(inject_spec, data)
        self.assertEqual(data, expect)
