from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobimport.__main__ import handle_import_msg, SERVICEDEFINITION


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
    def test_handle_prepare_msg(self, mock_get_mapping, mock_import_client):
        mock_import_client_instance = MagicMock()
        mock_import_client.return_value = mock_import_client_instance
        mock_get_mapping.return_value = "mapped_file"
        handle_import_msg(self.mock_msg)

        # Assert 'dataset_file' in body has priority over header 'dataset_file'
        mock_get_mapping.assert_called_with('data/somefile.json')

        mock_import_client.assert_called_with(dataset="mapped_file", msg=self.mock_msg)
        mock_import_client_instance.start_import_process.assert_called_once()

    @patch("gobimport.__main__.ImportClient")
    @patch("gobimport.__main__.get_mapping")
    def test_handle_prepare_msg_dataset_from_header(self, mock_get_mapping, mock_import_client):
        mock_import_client_instance = MagicMock()
        mock_import_client.return_value = mock_import_client_instance
        mock_get_mapping.return_value = "mapped_file"

        del self.mock_msg['dataset_file']

        handle_import_msg(self.mock_msg)

        mock_get_mapping.assert_called_with('data/fromheader.json')
        mock_import_client.assert_called_with(dataset="mapped_file", msg=self.mock_msg)
        mock_import_client_instance.start_import_process.assert_called_once()

    def test_handle_prepare_msg_without_dataset(self):
        del self.mock_msg['dataset_file']
        del self.mock_msg['header']['dataset_file']

        with self.assertRaises(AssertionError):
            handle_import_msg(self.mock_msg)

    @patch("gobimport.__main__.messagedriven_service")
    def test_main_entry(self, mock_messagedriven_service):
        from gobimport import __main__ as module
        with patch.object(module, "__name__", "__main__"):
            module.init()
            mock_messagedriven_service.assert_called_with(SERVICEDEFINITION, "Import")
