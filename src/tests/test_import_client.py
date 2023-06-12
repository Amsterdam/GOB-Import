from unittest import TestCase
from unittest.mock import MagicMock, patch, call, mock_open

from gobcore.enum import ImportMode

from gobimport import gob_model
from gobimport.import_client import ImportClient
from tests import fixtures


mock_model = MagicMock(spec_set=gob_model)


@patch('gobimport.converter.gob_model', mock_model)
@patch('gobimport.validator.gob_model', mock_model)
class TestImportClient(TestCase):

    import_client = None

    def setUp(self):
        self.mock_dataset = {
            'source': {
                'entity_id': fixtures.random_string(),
                'application': fixtures.random_string(),
                'name': fixtures.random_string(),
                'type': 'file',
                'config': {},
                'query': fixtures.random_string(),
            },
            'version': 0.1,
            'catalogue': fixtures.random_string(),
            'entity': fixtures.random_string(),
            'gob_mapping': {}
        }
        mock_model.__getitem__.return_value = {
                'collections': {
                    self.mock_dataset['entity']: {'all_fields': {}},
                }
            }

        self.mock_msg = {
            'header': {}
        }

    def test_init(self):
        logger = MagicMock()
        self.import_client = ImportClient(self.mock_dataset, self.mock_msg, logger)

        logger.info.assert_called()

    def test_publish(self):
        logger = MagicMock()
        self.import_client = ImportClient(self.mock_dataset, self.mock_msg, logger)
        self.import_client.n_rows = 10
        self.import_client.filename = "filename"
        msg = self.import_client.get_result_msg()
        self.assertEqual(msg['contents_ref'], 'filename')
        self.assertEqual(msg['summary']['num_records'], 10)
        self.assertEqual(msg['header']['version'], 0.1)

    def test_publish_delete_mode(self):
        logger = MagicMock()
        self.import_client = ImportClient(self.mock_dataset, self.mock_msg, logger, ImportMode.DELETE)
        self.import_client.n_rows = 0
        self.import_client.filename = "filename"

        app = self.mock_dataset['source']['application']
        entity = self.mock_dataset['entity']

        msg1 = f"Import dataset {entity} from {app} (mode = DELETE) started"
        logger.info.assert_called_with(msg1)

        result_msg = self.import_client.get_result_msg()
        self.assertTrue(result_msg["header"]["full"])

        msg2 = f"Import dataset {entity} from {app} completed. " \
               "0 records imported, all known entities will be marked as deleted."
        logger.info.assert_called_with(msg2, kwargs={'data': {'num_records': 0}})

    @patch('gobimport.import_client.Reader', autospec=True)
    def test_import_rows(self, mock_Reader):
        mock_reader = MagicMock()
        mock_Reader.return_value = mock_reader
        rows = ((1, 2), (3, 4))
        mock_reader.__enter__.return_value.read.return_value = rows

        progress = MagicMock()
        write = MagicMock()

        _self = MagicMock()
        _self.logger = MagicMock()
        _self.injector.inject = MagicMock()
        _self.merger = MagicMock()
        _self.converter = MagicMock()
        entity = 'Entity'
        _self.converter.convert.return_value = entity
        _self.validator = MagicMock()
        ImportClient.import_rows(_self, write, progress)
        _self.logger.info.assert_called()
        self.assertEqual(_self.injector.inject.call_args_list, [call(c) for c in rows])
        self.assertEqual(_self.merger.merge.call_args_list, [call(c, write) for c in rows])
        self.assertEqual(_self.converter.convert.call_args_list, [call(c) for c in rows])
        self.assertEqual(_self.validator.validate.call_args_list, [call(entity) for c in rows])
        self.assertEqual(write.call_args_list, [call(entity) for c in rows])

        _self.validator.result.called_once_with()
        self.assertEqual(len(_self.logger.info.call_args_list), 3)

        mock_reader.__exit__.assert_called()

        # exception
        mock_reader.reset_mock()
        _self.injector.inject.side_effect = Exception
        with self.assertRaises(Exception):
            ImportClient.import_rows(_self, write, progress)
        mock_reader.__exit__.assert_called()

    @patch('gobimport.import_client.Reader')
    def test_import_rows_merged(self, mock_Reader):
        mock_reader = MagicMock()
        mock_Reader.return_value = mock_reader
        rows = ((1, 2), (3, 4))
        mock_reader.__enter__.return_value.read.return_value = rows

        progress = MagicMock()
        write = MagicMock()

        _self = MagicMock()
        _self.converter.convert.return_value = 'Entity'

        _self.merger.is_merged = lambda x: True

        ImportClient.import_rows(_self, write, progress)
        _self.entity_validator.validate.assert_called_with("Entity", merged=True)

    @patch('gobimport.import_client.Reader')
    def test_import_row_too_few_records(self, mock_Reader):
        reader = MagicMock()
        mock_Reader.return_value = reader
        rows = set()
        reader.read.return_value = rows

        progress = MagicMock()
        write = MagicMock()

        _self = MagicMock()
        _self.mode = ImportMode.FULL
        _self.dataset = {}
        ImportClient.import_rows(_self, write, progress)

        _self.validator.result.assert_called_once_with()
        self.assertEqual(len(_self.logger.info.call_args_list), 3)
        self.assertEqual(len(_self.logger.error.call_args_list), 1)

    @patch('gobimport.import_client.ContentsWriter')
    @patch('gobimport.import_client.ProgressTicker')
    def test_import_dataset(self, mock_ProgressTicker, mock_ContentsWriter):
        _self = MagicMock()
        _self.get_result_msg.return_value = 'res'
        writer = MagicMock()
        mock_ContentsWriter.return_value.__enter__.return_value = writer
        filename = 'fname'
        writer.filename = filename
        writer.write = 'write'
        progress = MagicMock()
        mock_ProgressTicker.return_value.__enter__.return_value = progress

        ImportClient.import_dataset(_self)

        mock_ProgressTicker.called_once()
        _self.merger.prepare.assert_called_once_with(progress)
        self.assertEqual(_self.filename, filename)
        _self.import_rows.assert_called_once_with('write', progress)
        _self.merger.finish.assert_called_once_with('write')
        _self.entity_validator.result.assert_called_once()

    def test_import_dataset_mode_delete(self):
        _self = MagicMock()
        _self.mode = ImportMode.DELETE

        with patch("builtins.open", mock_open(read_data="")) as m:
            ImportClient.import_dataset(_self)

            m.assert_called_once()
            m.mock_calls[1] = call().close()

        _self.merger.assert_not_called()
        _self.import_rows.assert_not_called()
        _self.entity_validator.assert_not_called()

    @patch('gobimport.import_client.ContentsWriter')
    @patch('gobimport.import_client.traceback')
    def test_import_dataset_exception(self, mock_traceback, mock_ContentsWriter):
        _self = MagicMock()
        _self.get_result_msg.return_value = 'res'
        writer = MagicMock()
        writer.side_effect = Exception('Boom')
        mock_ContentsWriter.return_value.__enter__ = writer

        with self.assertRaises(Exception):
            ImportClient.import_dataset(_self)

            self.assertEqual(len(_self.logger.error.call_args_list), 2)
            mock_traceback.format_exc.assert_called_once_with(limit=-5)

    @patch("gobimport.import_client.ImportClient.get_result_msg")
    @patch("gobimport.import_client.ImportClient.import_dataset")
    def test_contextmanager(self, mock_import_dataset, mock_get_result_msg):
        logger = MagicMock()
        client = ImportClient(self.mock_dataset, self.mock_msg, logger)

        mock_import_dataset.side_effect = Exception

        # no exception, with logs
        with client:
            client.raise_exception = False
            client.import_dataset()

        logger.error.assert_called()

        logger.error.reset_mock()

        # exception, with logs
        with self.assertRaises(Exception):
            with client:
                client.raise_exception = True
                client.import_dataset()
        logger.error.assert_called()

        logger.error.reset_mock()
        mock_import_dataset.side_effect = [{}, {}]

        # no exception, no logs
        with client:
            client.raise_exception = False
            client.import_dataset()

        # no excption, no logs
        with client:
            client.raise_exception = True
            client.import_dataset()

        logger.error.assert_not_called()

        mock_get_result_msg.assert_not_called()
