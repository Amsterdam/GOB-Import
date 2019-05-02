from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobcore.exceptions import GOBException
from gobimport.__main__ import handle_import_msg, SERVICEDEFINITION, extract_dataset_from_msg,\
    _extract_dataset_variable


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

        mock_import_client.assert_called_with(dataset="mapped_file", msg=self.mock_msg)
        mock_import_client_instance.import_dataset.assert_called_once()

    def test_extract_dataset_variable_str(self):
        dataset = "somedataset.json"
        self.assertEqual(dataset, _extract_dataset_variable(dataset))

    @patch("gobimport.__main__.get_dataset_file_location")
    def test_extract_dataset_variable_dict(self, mock_get_dataset_file_location):
        dataset = {
            'catalogue': 'somecatalogue',
            'collection': 'somecollection',
            'application': 'someapplication',
        }
        mock_get_dataset_file_location.return_value = 'returned_dataset_location.json'
        result = _extract_dataset_variable(dataset)
        self.assertEqual(mock_get_dataset_file_location.return_value, result)
        mock_get_dataset_file_location.assert_called_with("somecatalogue", "somecollection", "someapplication")

    def test_extract_dataset_variable_dict_invalid(self):
        valid_dataset = {
            'catalogue': 'somecatalogue',
            'collection': 'somecollection',
            'application': 'someapplication',
        }

        for k in valid_dataset.keys():
            invalid_dataset = { key: valid_dataset[key] for key in valid_dataset.keys() if key is not k }

            with self.assertRaisesRegexp(GOBException, 'Missing dataset keys'):
                _extract_dataset_variable(invalid_dataset)

    def test_extract_dataset_variable_invalid_type(self):
        with self.assertRaisesRegexp(GOBException, 'Dataset of invalid type'):
            _extract_dataset_variable([])

    @patch("gobimport.__main__._extract_dataset_variable")
    def test_extract_dataset_from_msg(self, mock_extract_dataset_variable):
        testcases = [
            (
                {
                    "dataset": "dataset1.json",
                    "contents": {
                        "dataset": "dataset2.json"
                    },
                },
                "dataset1.json",
                "Data set file in message root should have priority",
            ),
            (
                {
                    "contents": {
                        "dataset": "dataset2.json"
                    },
                },
                "dataset2.json",
                "Data set in contents should have second priority",
            )
        ]

        for message, result, error_message in testcases:
            extract_dataset_from_msg(message)
            mock_extract_dataset_variable.assert_called_with(result)

    @patch("gobimport.__main__.messagedriven_service")
    def test_main_entry(self, mock_messagedriven_service):
        from gobimport import __main__ as module
        with patch.object(module, "__name__", "__main__"):
            module.init()
            mock_messagedriven_service.assert_called_with(SERVICEDEFINITION, "Import")
