from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.model import GOBModel
from gobimport.import_client import ImportClient
from tests import fixtures


@patch.object(GOBModel, 'has_states', MagicMock())
@patch('gobimport.import_client.logger')
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
            'catalogue': fixtures.random_string(),
            'entity': fixtures.random_string(),
            'gob_mapping': {}
        }

        self.mock_msg = {
            'header': {}
        }

    def test_init(self, mock_logger):
        self.import_client = ImportClient(self.mock_dataset, self.mock_msg)
        # Expect a process_id is created
        self.assertTrue(self.import_client.process_id)

        # Assert the logger is configured and called
        mock_logger.set_name.assert_called()
        mock_logger.set_default_args.assert_called()
        mock_logger.info.assert_called()

    def test_connect(self, mock_logger):
        test_connect_types = [
            ('gobimport.import_client.connect_to_file', 'file'),
            ('gobimport.import_client.connect_to_database', 'database'),
            ('gobimport.import_client.connect_to_oracle', 'oracle'),
            ('gobimport.import_client.connect_to_objectstore', 'objectstore'),
        ]

        self.import_client = ImportClient(self.mock_dataset, self.mock_msg)

        for connect_type in test_connect_types:
             with patch(connect_type[0]) as mock_connect:
                 mock_connect.return_value = (MagicMock, 'user')

                 self.import_client.source['type'] = connect_type[1]
                 self.import_client.connect()

                 mock_connect.assert_called()

        # Assert not implemented is raised with undefined connection type
        with self.assertRaises(NotImplementedError):
            self.import_client.source['type'] = fixtures.random_string()
            self.import_client.connect()

    def test_read(self, mock_logger):
        test_read_types = [
            ('gobimport.import_client.read_from_file', 'file'),
            ('gobimport.import_client.read_from_database', 'database'),
            ('gobimport.import_client.read_from_oracle', 'oracle'),
            ('gobimport.import_client.read_from_objectstore', 'objectstore'),
        ]

        self.import_client = ImportClient(self.mock_dataset, self.mock_msg)

        for read_type in test_read_types:
             with patch(read_type[0]) as mock_read:
                 mock_read.return_value = []

                 self.import_client.source['type'] = read_type[1]
                 self.import_client.read()

                 mock_read.assert_called()

        # Assert not implemented is raised with undefined read type
        with self.assertRaises(NotImplementedError):
            self.import_client.source['type'] = fixtures.random_string()
            self.import_client.read()
