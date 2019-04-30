from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobcore.exceptions import GOBException
from gobimport.__main__ import handle_import_msg, SERVICEDEFINITION, extract_dataset_from_msg


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
    def test_handle_prepare_msg(self, mock_extract_dataset, mock_get_mapping, mock_import_client):
        mock_import_client_instance = MagicMock()
        mock_import_client.return_value = mock_import_client_instance
        mock_extract_dataset.return_value = "dataset_file"
        mock_get_mapping.return_value = "mapped_file"
        handle_import_msg(self.mock_msg)

        mock_extract_dataset.assert_called_with(self.mock_msg)
        mock_get_mapping.assert_called_with("dataset_file")

        mock_import_client.assert_called_with(dataset="mapped_file", msg=self.mock_msg)
        mock_import_client_instance.import_dataset.assert_called_once()

    @patch("gobimport.__main__.get_dataset_file_location")
    def test_extract_dataset_from_msg(self, mock_get_dataset_file_location):
        def side_effect(arg):
            vals = {"data:set:3": "dataset3.json", "data:set:4": "dataset4.json"}
            return vals[arg]

        mock_get_dataset_file_location.side_effect = side_effect
        testcases = [
            (
                {
                    "dataset_file": "dataset1.json",
                    "contents": {
                        "dataset_file": "dataset2.json",
                        "dataset": "data:set:4"
                    },
                    "dataset": "data:set:3",
                },
                "dataset1.json",
                "Data set file in message root should have priority",
            ),
            (
                {
                    "contents": {
                        "dataset_file": "dataset2.json",
                        "dataset": "data:set:4"
                    },
                    "dataset": "data:set:3",
                },
                "dataset2.json",
                "Data set file in contents should have priority",
            ),
            (
                {
                    "contents": {
                        "dataset": "data:set:4"
                    },
                    "dataset": "data:set:3",
                },
                "dataset3.json",
                "Dataset in message root should have priority",
            ),
            (
                {
                    "contents": {
                        "dataset": "data:set:4"
                    },
                },
                "dataset4.json",
                "Should return dataset referenced in contents"
            ),
        ]

        for message, result, error_message in testcases:
            self.assertEqual(result, extract_dataset_from_msg(message), error_message)

        with self.assertRaisesRegexp(GOBException, 'Missing dataset file'):
            extract_dataset_from_msg({})

    @patch("gobimport.__main__.messagedriven_service")
    def test_main_entry(self, mock_messagedriven_service):
        from gobimport import __main__ as module
        with patch.object(module, "__name__", "__main__"):
            module.init()
            mock_messagedriven_service.assert_called_with(SERVICEDEFINITION, "Import")
