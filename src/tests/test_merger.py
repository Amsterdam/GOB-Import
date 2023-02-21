import unittest

from unittest import mock

from gobimport.import_client import ImportClient
from gobimport.merger import Merger


class TestMerger(unittest.TestCase):

    def setUp(self):
        pass

    def test_constructor(self):
        merger = Merger("Any import client")
        self.assertEqual(merger.merge_def, {})
        self.assertEqual(merger.merge_items, {})

    def test_merge_diva_into_dgdialog(self):
        written = []

        merger = Merger("Any import client")
        merger.merge_def = {
            "copy": ["a", "b"]
        }

        entities = [
            {
                "a": 11,
                "b": 12,
                "c": 13,
                "volgnummer": 1
            },
            {
                "a": 31,
                "b": 32,
                "c": 33,
                "volgnummer": 3
            },
            {
                "a": 21,
                "b": 22,
                "c": 23,
                "volgnummer": 2
            }
        ]
        entity = {
            "a": None,
            "b": None,
            "c": None,
            "volgnummer": 1
        }
        write = lambda e: written.append(e)

        merger._merge_diva_into_dgdialog(entity, write, entities)
        self.assertEqual(entity, {
            "a": 31,
            "b": 32,
            "c": None,
            "volgnummer": 3
        })

        entity = {
            "a": None,
            "b": None,
            "c": None,
            "volgnummer": 2
        }
        merger._merge_diva_into_dgdialog(entity, write, entities)
        self.assertEqual(entity, {
            "a": 31,
            "b": 32,
            "c": None,
            "volgnummer": 4
        })

        # Remember that entities get sorted inside the merge method!
        self.assertEqual(written, [entities[0], entities[1]])

    def test_prepare_no_merge_def(self):
        mock_client = mock.MagicMock(spec=ImportClient)
        mock_client.source = {}
        merger = Merger(mock_client)
        merger.prepare(progress=None)
        self.assertEqual(merger.merge_def, {})

    @mock.patch('gobimport.merger.get_import_definition_by_filename', mock.MagicMock())
    def test_prepare_with_merge_def(self):
        mock_client = mock.MagicMock(spec=ImportClient)
        mock_client.source = {
            "merge": {
                "dataset": 123,
                "id": "diva_into_dgdialog"
            }
        }
        mock_client.dataset = {}
        merger = Merger(mock_client)
        merger.prepare(progress=None)
        self.assertEqual(merger.merge_def, mock_client.source["merge"])

    def test_collect_entity(self):
        merger = Merger(None)
        merge_def = {
            "on": "any on"
        }
        merger._collect_entity({
            "any on": "a"
        }, merge_def)
        self.assertEqual(merger.merge_items, {'a': {'entities': [{'any on': 'a'}]}})
        merger._collect_entity({
            "any on": "b"
        }, merge_def)
        self.assertEqual(merger.merge_items["b"], {'entities': [{'any on': 'b'}]})
        merger._collect_entity({
            "any on": "b"
        }, merge_def)
        self.assertEqual(len(merger.merge_items["b"]["entities"]), 2)

    def test_is_merged(self):
        merger = Merger(None)
        entity = {"b": "value2", "volgnummer": 1}
        self.assertFalse(merger.is_merged(entity))

        merger.merge_def = {"on": "b"}
        self.assertFalse(merger.is_merged(entity))

        merger.merged = {"value2"}
        self.assertFalse(merger.is_merged(entity))

        merger.merge_items["value2"] = {"entities": []}
        self.assertFalse(merger.is_merged(entity))

        merger.merge_items["value2"] = {"entities": [entity]}
        self.assertTrue(merger.is_merged(entity))

    @mock.patch('gobimport.merger.get_import_definition_by_filename', mock.MagicMock())
    def test_merge(self):
        mock_client = mock.MagicMock(spec=ImportClient)
        mock_client.source = {
            "merge": {
                "dataset": 123,
                "id": "diva_into_dgdialog",
                "on": "any on",
                "copy": "a"
            }
        }
        mock_client.dataset = {}
        merger = Merger(mock_client)
        merger.prepare(progress=None)
        entity = {"any on": 1, "a": None, "volgnummer": 1}
        merger.merge(entity, lambda e: None)
        self.assertEqual(entity, {"any on": 1, "a": None, "volgnummer": 1})

        merger.merge_items[1] = {
            "entities": [{"any on": 1, "a": 2, "volgnummer": 1}]
        }
        merger.merge_items[2] = {
            "entities": [{"any on": 2, "a": 2, "volgnummer": 1}]
        }

        merger.merge(entity, lambda e: None)
        self.assertEqual(entity, {"any on": 1, "a": 2, "volgnummer": 1})
        self.assertIsNotNone(merger.merge_items[2])
        self.assertIsNotNone(merger.merge_items[1])
        self.assertEqual(len(merger.merge_items.keys()), 2)

        finished = []
        merger.finish(lambda e: finished.append(e))
        self.assertIsNone(merger.merge_items.get(1))
        self.assertIsNone(merger.merge_items.get(2))
        self.assertEqual(len(finished), 1)
