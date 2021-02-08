from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.exceptions import GOBException

from gobimport.__main__ import ImportMode, MUTATIONS_IMPORT_CALLBACK_KEY, MUTATIONS_IMPORT_CALLBACK_QUEUE, \
    SERVICEDEFINITION, WORKFLOW_EXCHANGE, extract_dataset_from_msg, handle_import_msg, handle_import_object_msg, \
    handle_mutation_import_callback
from gobimport.database.model import MutationImport


class TestMain(TestCase):

    def setUp(self):
        self.mock_msg = {
            'dataset_file': 'data/somefile.json',
            'header': {
                'dataset_file': 'data/fromheader.json',
            },
        }

    @patch("gobimport.__main__.MutationsHandler")
    @patch("gobimport.__main__.logger")
    @patch("gobimport.__main__.ImportClient")
    @patch("gobimport.__main__.extract_dataset_from_msg")
    def test_handle_import_msg(self, mock_extract_dataset, mock_import_client, mock_logger, mock_mutations_handler):
        """Tests handle_import_msg for a normal import, without mutations

        :param mock_extract_dataset:
        :param mock_import_client:
        :param mock_logger:
        :param mock_mutations_handler:
        :return:
        """
        mock_import_client_instance = MagicMock()
        mock_import_client.return_value = mock_import_client_instance
        mock_mutations_handler.is_mutations_import.return_value = False
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
        mock_mutations_handler.is_mutations_import.assert_called_with(mock_extract_dataset.return_value)

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

    @patch("gobimport.__main__.DatabaseSession")
    @patch("gobimport.__main__.MutationImportRepository")
    @patch("gobimport.__main__.MutationsHandler")
    @patch("gobimport.__main__.logger")
    @patch("gobimport.__main__.ImportClient")
    @patch("gobimport.__main__.extract_dataset_from_msg")
    def test_handle_import_msg_mutations(self, mock_extract_dataset, mock_import_client, mock_logger,
                                         mock_mutations_handler, mock_repo, mock_session):

        mock_extract_dataset.return_value = {
            "source": {
                "name": "Some name",
                "application": "APP NAME",
            },
            "catalogue": "CAT",
            "entity": "ENT"
        }
        mock_mutations_handler.is_mutations_import.return_value = True

        mocked_last_import = MutationImport()
        mocked_next_import = MutationImport()
        mocked_next_import.id = 42
        mocked_next_import.mode = ImportMode.MUTATIONS

        mock_repo.return_value.get_last.return_value = mocked_last_import
        mock_mutations_handler.return_value.get_next_import.return_value = (mocked_next_import, 'UPDATED DATASET')
        handle_import_msg(self.mock_msg)

        mock_mutations_handler.is_mutations_import.assert_called_with(mock_extract_dataset.return_value)
        mock_repo.return_value.get_last.assert_called_with('CAT', 'ENT', 'APP NAME')
        mock_repo.return_value.save.assert_called_with(mocked_next_import)

        mock_import_client.assert_called_with(
            dataset='UPDATED DATASET',
            msg=self.mock_msg,
            mode=ImportMode.MUTATIONS,
            logger=mock_logger,
        )

        self.assertEqual({
            'dataset_file': 'data/somefile.json',
            'header': {
                'application': 'APP NAME',
                'catalogue': 'CAT',
                'dataset_file': 'data/fromheader.json',
                'entity': 'ENT',
                'source': 'Some name',
                'on_workflow_complete': {
                    'exchange': WORKFLOW_EXCHANGE,
                    'key': MUTATIONS_IMPORT_CALLBACK_KEY,
                },
                'mutation_import_id': 42,
                'mode': ImportMode.MUTATIONS.value,
            }

        }, self.mock_msg)


    @patch("gobimport.__main__.DatabaseSession")
    @patch("gobimport.__main__.MutationImportRepository")
    @patch("gobimport.__main__.MutationsHandler")
    @patch("gobimport.__main__.extract_dataset_from_msg")
    @patch("gobimport.__main__.start_workflow")
    @patch("gobimport.__main__.logger", MagicMock())
    def test_handle_mutation_import_callback(self, mock_start_workflow, mock_extract_dataset, mock_mutations_handler,
                                             mock_repo, mock_db_session):
        msg = {
            "header": {
                "mutation_import_id": 42,
                "catalogue": "THE CAT",
                "collection": "THE COLL",
                "entity": "THE ENT",
                "application": "THE APP",
            },
        }

        # Test start next workflow
        mocked_session = mock_db_session.return_value.__enter__.return_value
        mutation_import = MutationImport()
        mock_repo.return_value.get.return_value = mutation_import
        self.assertIsNone(mutation_import.ended_at)
        mock_mutations_handler.return_value.have_next.return_value = True

        handle_mutation_import_callback(msg)

        mock_repo.assert_called_with(mocked_session)
        mock_repo.return_value.get.assert_called_with(42)
        mock_repo.return_value.save.assert_called_with(mutation_import)
        self.assertIsNotNone(mutation_import.ended_at)
        mock_mutations_handler.assert_called_with(mock_extract_dataset.return_value)

        mock_start_workflow.assert_called_with({
            'workflow_name': 'import',
        }, {
            "catalogue": "THE CAT",
            "collection": "THE COLL",
            "entity": "THE ENT",
            "application": "THE APP",
        })

        # No action
        mock_mutations_handler.return_value.have_next.return_value = False
        mock_start_workflow.reset_mock()

        handle_mutation_import_callback(msg)
        mock_start_workflow.assert_not_called()

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

    @patch("gobimport.__main__.create_queue_with_binding")
    @patch("gobimport.__main__.messagedriven_service")
    def test_main_entry(self, mock_messagedriven_service, mock_create_queue):
        from gobimport import __main__ as module
        with patch.object(module, "__name__", "__main__"):
            module.init()
            mock_create_queue.assert_called_with(
                exchange=WORKFLOW_EXCHANGE,
                queue=MUTATIONS_IMPORT_CALLBACK_QUEUE,
                key=MUTATIONS_IMPORT_CALLBACK_KEY
            )
            mock_messagedriven_service.assert_called_with(SERVICEDEFINITION, "Import")
