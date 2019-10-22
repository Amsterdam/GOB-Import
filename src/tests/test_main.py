from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobcore.exceptions import GOBException
from gobimport.__main__ import handle_import_msg, SERVICEDEFINITION, extract_dataset_from_msg
from gobimport.config import FULL_IMPORT


class TestMain(TestCase):

    def setUp(self):
        self.mock_msg = {
            'dataset_file': 'data/somefile.json',
            'header': {
                'dataset_file': 'data/fromheader.json',
            }
        }

    @patch("gobimport.__main__.ImportClient")
    @patch("gobimport.__main__.get_mapping")
    @patch("gobimport.__main__.extract_dataset_from_msg")
    def test_handle_import_msg(self, mock_extract_dataset, mock_get_mapping, mock_import_client):
        mock_import_client_instance = MagicMock()
        mock_import_client.return_value = mock_import_client_instance
        mock_extract_dataset.return_value = "dataset_file"
        mock_get_mapping.return_value = "mapped_file"
        handle_import_msg(self.mock_msg)

        mock_extract_dataset.assert_called_with(self.mock_msg)
        mock_get_mapping.assert_called_with("dataset_file")

        mock_import_client.assert_called_with(dataset="mapped_file", msg=self.mock_msg, mode=FULL_IMPORT)
        mock_import_client_instance.import_dataset.assert_called_once()

    @patch("gobimport.__main__.get_dataset_file_location")
    def test_extract_dataset_from_msg(self, mock_dataset_file_location):
        msg = {
            'header': {
                'catalogue': 'cat',
                'collection': 'coll',
                'application': 'app'
            }
        }

        result = extract_dataset_from_msg(msg)
        self.assertEqual(mock_dataset_file_location.return_value, result)

        mock_dataset_file_location.assert_called_with('cat', 'coll', 'app')

        msg = {
            'header': {
                'catalogue': 'cat',
                'collection': 'coll'
            }
        }

        extract_dataset_from_msg(msg)
        mock_dataset_file_location.assert_called_with('cat', 'coll', None)

    def test_extract_dataset_missing_keys(self):
        cases = [
            {'catalogue': 'cat'},
            {'collection': 'col'},
            {},
        ]

        for case in cases:
            with self.assertRaises(GOBException):
                extract_dataset_from_msg({'header': case})

    @patch("gobimport.__main__.messagedriven_service")
    def test_main_entry(self, mock_messagedriven_service):
        from gobimport import __main__ as module
        with patch.object(module, "__name__", "__main__"):
            module.init()
            mock_messagedriven_service.assert_called_with(SERVICEDEFINITION, "Import")
