from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.exceptions import GOBException

from gobimport.__main__ import ImportMode, \
    SERVICEDEFINITION, extract_dataset_from_msg, handle_import_msg, handle_import_object_msg


class TestMain(TestCase):

    def setUp(self):
        self.mock_msg = {
            'dataset_file': 'data/somefile.json',
            'header': {
                'dataset_file': 'data/fromheader.json',
            },
        }

    @patch("gobimport.__main__.logger")
    @patch("gobimport.__main__.ImportClient")
    @patch("gobimport.__main__.extract_dataset_from_msg")
    def test_handle_import_msg(self, mock_extract_dataset, mock_import_client, mock_logger):
        """Tests handle_import_msg for a normal import

        :param mock_extract_dataset:
        :param mock_import_client:
        :param mock_logger:
        :return:
        """
        mock_import_client_instance = MagicMock()
        mock_import_client.return_value = mock_import_client_instance
        mock_extract_dataset.return_value = {
            "source": {
                "name": "Some name",
                "application": "The application",
            },
            "catalogue": "CAT",
            "entity": "ENT"
        }
        handle_import_msg(self.mock_msg)

        mock_extract_dataset.assert_called_with(self.mock_msg)

        mock_import_client.assert_called_with(
            dataset=mock_extract_dataset.return_value,
            msg=self.mock_msg,
            mode=ImportMode.FULL,
            logger=mock_logger,
        )
        mock_import_client_instance.import_dataset.assert_called_once()

        self.assertEqual({
            'dataset_file': 'data/somefile.json',
            'header': {
                'application': 'The application',
                'catalogue': 'CAT',
                'dataset_file': 'data/fromheader.json',
                'entity': 'ENT',
                'source': 'Some name'
            }

        }, self.mock_msg)

    @patch("gobimport.__main__.logger")
    @patch("gobimport.__main__.MappinglessConverterAdapter")
    def test_handle_import_object_msg(self, mock_converter, mock_logger):
        msg = {
            'header': {
                'catalogue': 'CAT',
                'entity': 'ENT',
                'entity_id_attr': 'id_attr',
            },
            'contents': {'the': 'contents'},
        }

        self.assertEqual({
            'header': {
                **msg['header'],
                'mode': 'single_object',
                'collection': 'ENT',
            },
            'summary': mock_logger.get_summary.return_value,
            'contents': [mock_converter.return_value.convert.return_value]
        }, handle_import_object_msg(msg))

        mock_converter.assert_called_with('CAT', 'ENT', 'id_attr')
        mock_converter.return_value.convert.assert_called_with({'the': 'contents'})

    @patch("gobimport.__main__.get_import_definition")
    def test_extract_dataset_from_msg(self, mock_import_definition):
        msg = {
            'header': {
                'catalogue': 'cat',
                'collection': 'coll',
                'application': 'app'
            }
        }

        result = extract_dataset_from_msg(msg)
        self.assertEqual(mock_import_definition.return_value, result)

        mock_import_definition.assert_called_with('cat', 'coll', 'app')

        msg = {
            'header': {
                'catalogue': 'cat',
                'collection': 'coll'
            }
        }

        extract_dataset_from_msg(msg)
        mock_import_definition.assert_called_with('cat', 'coll', None)

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
