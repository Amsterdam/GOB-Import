import json

from unittest import TestCase
from unittest.mock import patch, MagicMock

from gobcore.exceptions import GOBException
from gobimport.mapping import get_dataset_file_location, _build_dataset_locations_mapping

from collections import defaultdict


class TestMapping(TestCase):
    mock_locations_mapping = {
        "catalogue_a": {
            "collection_a": {
                "application_a": "cat_a_col_a_app_a.json",
            },
            "collection_b": {
                "application_a": "cat_a_col_b_app_a.json",
            },
        },
        "catalogue_b":{
            "collection_b": {
                "application_a": "cat_b_col_b_app_a.json",
            },
        },
    }

    @patch("gobimport.mapping.dataset_locations_mapping", mock_locations_mapping)
    def test_get_dataset_file_location(self):
        self.assertEqual(self.mock_locations_mapping["catalogue_b"]["collection_b"]["application_a"],
                         get_dataset_file_location("catalogue_b", "collection_b", "application_a"))

        with self.assertRaisesRegexp(GOBException, "No dataset found"):
            get_dataset_file_location("catalogue_b", "collection_c_non_existent", "application_a")

    def defaultdict_to_dict(self, o):
        if isinstance(o, defaultdict):
            o = {k: self.defaultdict_to_dict(v) for k, v in o.items()}
        return o

    @patch("gobimport.mapping.get_mapping")
    @patch("gobimport.mapping.os")
    @patch("gobimport.mapping.DATASET_DIR", "mocked/data/dir/")
    def test_build_dataset_locations_mapping(self, mock_os, mock_get_mapping):
        mock_os.listdir.return_value = ['file.json']
        mock_os.path.isfile.return_value = True
        mock_get_mapping.return_value = {
            'catalogue': 'somecatalogue',
            'entity': 'someentity',
            'source': {
                'application': 'some_application'
            }
        }

        expected_result = {
            'somecatalogue': {
                'someentity': {
                    'some_application': 'mocked/data/dir/file.json'
                }
            }
        }

        result = _build_dataset_locations_mapping()
        self.assertEqual(expected_result, self.defaultdict_to_dict(result))

    @patch("gobimport.mapping.get_mapping")
    @patch("gobimport.mapping.os")
    @patch("gobimport.mapping.DATASET_DIR", "mocked/data/dir/")
    def test_build_dataset_locations_mapping_invalid_dict(self, mock_os, mock_get_mapping):
        mock_os.listdir.return_value = ['file.json']
        mock_os.path.isfile.return_value = True
        mock_get_mapping.return_value = {
            'catalogue': 'somecatalogue',
            'source': {
                'application': 'some_application'
            }
        }

        with self.assertRaisesRegexp(GOBException, "Dataset file mocked/data/dir/file.json invalid"):
            _build_dataset_locations_mapping()

    @patch("gobimport.mapping.get_mapping")
    @patch("gobimport.mapping.os")
    @patch("gobimport.mapping.DATASET_DIR", "mocked/data/dir/")
    def test_build_dataset_locations_mapping_invalid_json(self, mock_os, mock_get_mapping):
        mock_os.listdir.return_value = ['file.json']
        mock_os.path.isfile.return_value = True
        mock_get_mapping.side_effect = json.decoder.JSONDecodeError("", MagicMock(), 0)

        with self.assertRaisesRegexp(GOBException, "Dataset file mocked/data/dir/file.json invalid"):
            _build_dataset_locations_mapping()
