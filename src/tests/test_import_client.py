from unittest import TestCase
from unittest.mock import MagicMock, patch

from gobcore.model import GOBModel
from gobimport.import_client import ImportClient
from tests import fixtures


@patch('gobimport.converter.GOBModel', MagicMock(spec=GOBModel))
@patch.object(GOBModel, 'has_states', MagicMock())
@patch.object(GOBModel, 'get_collection', MagicMock())
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
            'version': 0.1,
            'catalogue': fixtures.random_string(),
            'entity': fixtures.random_string(),
            'gob_mapping': {}
        }

        self.mock_msg = {
            'header': {}
        }


    def test_init(self, mock_logger):
        self.import_client = ImportClient(self.mock_dataset, self.mock_msg)

        # Assert the logger is configured and called
        mock_logger.configure.assert_called()
        mock_logger.info.assert_called()

    def test_publish(self, mock_logger):
        self.import_client = ImportClient(self.mock_dataset, self.mock_msg)
        self.import_client.n_rows = 10
        self.import_client.filename = "filename"
        msg = self.import_client.get_result_msg()
        self.assertEqual(msg['contents_ref'], 'filename')
        self.assertEqual(msg['summary']['num_records'], 10)
        self.assertEqual(msg['header']['version'], 0.1)



