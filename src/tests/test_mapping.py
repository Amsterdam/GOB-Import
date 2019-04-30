from unittest import TestCase
from unittest.mock import patch
from tests.fixtures import random_string

from gobcore.exceptions import GOBException
from gobimport.mapping import in_datadir, get_dataset_file_location


class TestMapping(TestCase):

    def test_in_datadir(self):
        arg = random_string()
        self.assertEqual("data/" + arg, in_datadir(arg))

    def test_get_dataset_file_location(self):
        mock_dataset_files = {
            "reference:a": "file_a.json",
            "reference:b": "file_b.json",
        }

        with patch("gobimport.mapping.DATASET_FILES", mock_dataset_files):
            for k, v in mock_dataset_files.items():
                self.assertEqual(v, get_dataset_file_location(k))

            with self.assertRaisesRegexp(GOBException, 'not found'):
                get_dataset_file_location("nonexistent:key")


