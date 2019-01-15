import unittest
from unittest import mock

from gobimport.reader.objectstore import read_from_objectstore, _read_xls

class MockExcel():

    def iterrows(self):
        return [
            (0, {"a": 1})
        ]

class TestReader(unittest.TestCase):

    def setUp(self):
        pass

    @mock.patch('gobimport.reader.objectstore.get_full_container_list')
    def test_read(self, mock_container_list):
        config = {
            "container": "container",
            "file_filter" : ".*",
            "file_type": None
        }
        data = read_from_objectstore(connection=None, config=config)
        self.assertEqual(data, [])
        mock_container_list.assert_called()

    @mock.patch('gobimport.reader.objectstore.get_full_container_list')
    def test_read_not_empty(self, mock_container_list):
        mock_container_list.return_value = [
            {
                "name": "name"
            }
        ]
        config = {
            "container": "container",
            "file_filter" : ".*",
            "file_type": None
        }
        data = read_from_objectstore(connection=None, config=config)
        self.assertEqual(data, [{"name": "name"}])

    @mock.patch('gobimport.reader.objectstore._read_xls')
    @mock.patch('gobimport.reader.objectstore.get_object')
    @mock.patch('gobimport.reader.objectstore.get_full_container_list')
    def test_read_from_xls(self, mock_container_list, mock_object, mock_read_xls):
        mock_read_xls.return_value = []
        mock_container_list.return_value = [
            {
                "name": "name"
            }
        ]
        config = {
            "container": "container",
            "file_filter" : ".*",
            "file_type": "XLS"
        }
        data = read_from_objectstore(connection=None, config=config)
        self.assertEqual(data, [])
        mock_read_xls.assert_called()

    @mock.patch('gobimport.reader.objectstore.io.BytesIO')
    @mock.patch('gobimport.reader.objectstore.pandas.read_excel')
    def test_read_xls(self, mock_read, mock_io):
        mock_read.return_value = MockExcel()
        result = _read_xls({})
        self.assertEqual(result, [{"a": 1}])

    @mock.patch('gobimport.reader.objectstore.pandas.isnull')
    @mock.patch('gobimport.reader.objectstore.io.BytesIO')
    @mock.patch('gobimport.reader.objectstore.pandas.read_excel')
    def test_read_xls_null_values(self, mock_read, mock_io, mock_isnull):
        mock_read.return_value = MockExcel()
        mock_isnull.return_value = True
        result = _read_xls({})
        self.assertEqual(result, [])