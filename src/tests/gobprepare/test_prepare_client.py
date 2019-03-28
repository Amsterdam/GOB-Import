from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobprepare.prepare_client import PrepareClient
from tests import fixtures


@patch('gobprepare.prepare_client.logger')
@patch("gobprepare.prepare_client.OracleToPostgresCloner", autospec=True)
class TestPrepareClient(TestCase):

    def setUp(self):
        self.mock_dataset = {
            'source': {
                'application': fixtures.random_string(),
                'type': 'oracle',
                'schema': fixtures.random_string()
            },
            'destination': {
                'application': fixtures.random_string(),
                'schema': fixtures.random_string()
            },
            'action': {
                'type': 'clone',
                'mask': {
                    "sometable": {
                        "somecolumn": "mask"
                    }
                }
            }
        }

    def test_init(self, mock_cloner, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset)
        # Expect a process_id is created
        self.assertTrue(prepare_client.process_id)

        # Assert the logger is configured and called
        mock_logger.set_name.assert_called()
        mock_logger.set_default_args.assert_called()
        mock_logger.info.assert_called()

    @patch("gobprepare.prepare_client.connect_to_oracle", return_value=["connection", "user"])
    @patch("gobprepare.prepare_client.connect_to_postgresql", return_value=["connection", "user"])
    def test_connect(self, mock_connect_postgres, mock_connect_oracle, mock_cloner, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset)
        # Reset counter
        mock_logger.info.reset_mock()
        prepare_client.connect()

        mock_connect_oracle.assert_called_once()
        mock_connect_postgres.assert_called_once()
        self.assertEqual(2, mock_logger.info.call_count)

    def test_connect_invalid_source_type(self, mock_cloner, mock_logger):
        self.mock_dataset['source']['type'] = 'nonexistent'
        prepare_client = PrepareClient(self.mock_dataset)

        with self.assertRaises(NotImplementedError):
            prepare_client.connect()

    def test_clone(self, mock_cloner, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset)
        prepare_client.clone()

        mock_cloner.assert_called_with(
            None,
            self.mock_dataset['source']['schema'],
            None,
            self.mock_dataset['destination']['schema'],
            self.mock_dataset['action'],
        )

    def test_clone_invalid_source_type(self, mock_cloner, mock_logger):
        self.mock_dataset['source']['type'] = 'nonexistent'
        prepare_client = PrepareClient(self.mock_dataset)

        with self.assertRaises(NotImplementedError):
            prepare_client.clone()

    def test_prepare(self, mock_cloner, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset)
        prepare_client.clone = MagicMock()

        prepare_client.prepare()
        prepare_client.clone.assert_called_once()

    def test_prepare_invalid_action_type(self, mock_cloner, mock_logger):
        self.mock_dataset['action']['type'] = 'nonexistent'
        prepare_client = PrepareClient(self.mock_dataset)

        with self.assertRaises(NotImplementedError):
            prepare_client.prepare()

    def test_start_prepare_process(self, mock_cloner, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset)
        prepare_client.connect = MagicMock()
        prepare_client.prepare = MagicMock()

        prepare_client.start_prepare_process()
        prepare_client.connect.assert_called_once()
        prepare_client.prepare.assert_called_once()

    def test_start_prepare_process_exception(self, mock_cloner, mock_logger):
        prepare_client = PrepareClient(self.mock_dataset)
        prepare_client.connect = MagicMock(side_effect=Exception)

        prepare_client.start_prepare_process()
        mock_logger.error.assert_called_once()
