from io import StringIO
from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.exceptions import GOBException
from gobimport.__main__ import ImportMode, extract_dataset_from_msg, handle_import_msg, handle_import_object_msg, \
    SERVICEDEFINITION, main


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
    def test_handle_import_msg_broker(self, mock_extract_dataset, mock_import_client, mock_logger):
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

        mock_logger.add_message_broker_handler.assert_called_once()
        mock_extract_dataset.assert_called_with(self.mock_msg)

        mock_import_client.assert_called_with(
            dataset=mock_extract_dataset.return_value,
            msg=self.mock_msg,
            mode=ImportMode.FULL,
            logger=mock_logger,
        )
        mock_import_client_instance.import_dataset.assert_called_once()
        mock_import_client_instance.import_dataset.assert_called_with(None)

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
    @patch("gobimport.__main__.ImportClient")
    @patch("gobimport.__main__.extract_dataset_from_msg")
    def test_handle_import_msg_standalone(self, mock_extract_dataset, mock_import_client, mock_logger):
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
        handle_import_msg(self.mock_msg, use_message_broker=False, destination="new_dest")

        mock_logger.add_message_broker_handler.assert_not_called()
        mock_extract_dataset.assert_called_with(self.mock_msg)

        mock_import_client.assert_called_with(
            dataset=mock_extract_dataset.return_value,
            msg=self.mock_msg,
            mode=ImportMode.FULL,
            logger=mock_logger,
        )
        mock_import_client_instance.import_dataset.assert_called_with("new_dest")

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
    def test_main_entry_message_broker(self, mock_messagedriven_service):
        from gobimport.__main__ import sys

        with patch.object(sys, "argv", ["gobimport"]):
            main()
            mock_messagedriven_service.assert_called_with(SERVICEDEFINITION, "Import")

    @patch("gobimport.__main__.handle_import_msg")
    def test_main_entry_standalone(self, mock_handle):
        from gobimport.__main__ import sys

        with patch.object(sys, "argv", ["gobimport", "import", "bag", "ligplaatsen", "Neuron"]):
            main()
            mock_handle.assert_called_with(
                msg={
                    "header": {
                        "catalogue": "bag",
                        "collection": "ligplaatsen",
                        "application": "Neuron",
                        "mode": "full"
                    }
                },
                use_message_broker=False,
                destination="bag/ligplaatsen"
            )

        args = ["gobimport", "import", "bag", "ligplaatsen", "Neuron", "recent"]
        with patch.object(sys, "argv", args):
            main()
            mock_handle.assert_called_with(
                msg={
                    "header": {
                        "catalogue": "bag",
                        "collection": "ligplaatsen",
                        "application": "Neuron",
                        "mode": "recent"
                    }
                },
                use_message_broker=False,
                destination="bag/ligplaatsen"
            )

        with (
            patch.object(sys, "argv", ["gobimport", "import", "gebieden"]),
            patch('sys.stderr', new_callable=StringIO) as mock_stderr
        ):
            err = "gobimport: error: the following arguments are required: collection"
            self.assertRaisesRegex(SystemExit, "2", main)
            self.assertEqual(err, mock_stderr.getvalue().splitlines()[-1])

        with (
            patch.object(sys, "argv", ["gobimport", "import"]),
            patch('sys.stderr', new_callable=StringIO) as mock_stderr
        ):
            err = "gobimport: error: the following arguments are required: catalogue, collection"
            self.assertRaisesRegex(SystemExit, "2", main)
            self.assertEqual(err, mock_stderr.getvalue().splitlines()[-1])
