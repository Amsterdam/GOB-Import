import os
import pandas
from gobimport.file import connect_to_file, read_from_file

from unittest import TestCase
from unittest.mock import patch


class TestFile(TestCase):

    @patch("gobimport.file.pandas.read_csv")
    def test_connect_to_file(self, mock_read_csv):
        config = {
            'filename': 'filename.csv',
            'filetype': 'CSV',
            'separator': 'sep',
            'encoding': 'enc',
        }
        file_path = 'the filepath'

        mock_read_csv.return_value = "readcsvresult"

        result = connect_to_file(config, file_path)

        mock_read_csv.assert_called_with(filepath_or_buffer=file_path,
                                         sep=config['separator'],
                                         encoding=config['encoding'],
                                         dtype=str)

        self.assertEqual(("readcsvresult", ""), result)

    def test_connect_to_file_invalid_filetype(self):
        config = {
            'filename': 'somefilename.csv',
            'filetype': 'TXT',
        }

        with self.assertRaises(NotImplementedError):
            connect_to_file(config, 'file path')

    def test_read_from_file(self):
        filepath = os.path.join(os.path.dirname(__file__), 'fixtures', 'simple_csv.csv')
        csv_handle = pandas.read_csv(
            filepath_or_buffer=filepath,
            sep=',',
            encoding='utf-8',
        )

        result = read_from_file(csv_handle)
        expected_result = [
            {'id': 1, 'firstname': 'Sheldon', 'lastname': 'Cooper'},
            {'id': 2, 'firstname': 'Rajesh', 'lastname': 'Koothrappali'},
            {'id': 3, 'firstname': None, 'lastname': None},
        ]

        result = [dict(row) for row in result]
        self.assertEqual(expected_result, result)
