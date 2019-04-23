import unittest

from unittest import mock

from gobimport.reader import Reader

@mock.patch('gobimport.reader.logger', mock.MagicMock())
class TestReader(unittest.TestCase):

    def setUp(self):
        self.source = {
            "type": "any type",
            "application": "any application",
            "query": "any query",
            "config": "any config",
            "any other item": "something"
        }
        self.app = "Any app"

    def test_constructor(self):
        reader = Reader(self.source, self.app)
        self.assertEqual(reader.app, self.app)
        self.assertEqual(reader.source, self.source)
        self.assertEqual(reader._connection, None)

    @mock.patch("gobimport.reader.get_database_config")
    @mock.patch("gobimport.reader.get_objectstore_config")
    def test_connect(self, mock_objectstore_config, mock_database_config):
        test_connect_types = [
            ('gobimport.reader.connect_to_file', 'file'),
            ('gobimport.reader.connect_to_database', 'database'),
            ('gobimport.reader.connect_to_oracle', 'oracle'),
            ('gobimport.reader.connect_to_objectstore', 'objectstore'),
            ('gobimport.reader.connect_to_postgresql', 'postgres'),
        ]

        reader = Reader(self.source, self.app)
        for connect_type in test_connect_types:
             with mock.patch(connect_type[0]) as mock_connect:
                 mock_connect.return_value = (mock.MagicMock, 'user')

                 reader.source['type'] = connect_type[1]
                 reader.connect()

                 self.assertIsNotNone(reader._connection)

                 mock_connect.assert_called()

        # Assert not implemented is raised with undefined connection type
        with self.assertRaises(NotImplementedError):
            reader.source['type'] = "any type"
            reader.connect()

    def test_read(self):
        test_read_types = [
            ('gobimport.reader.query_file', 'file'),
            ('gobimport.reader.query_database', 'database'),
            ('gobimport.reader.query_oracle', 'oracle'),
            ('gobimport.reader.query_objectstore', 'objectstore'),
            ('gobimport.reader.query_postgresql', 'postgres'),
        ]

        reader = Reader(self.source, self.app)
        reader._connection = "any connection"

        for read_type in test_read_types:
             with mock.patch(read_type[0]) as mock_read:

                 reader.source['type'] = read_type[1]

                 mock_read.return_value = []

                 reader.read()

                 mock_read.assert_called()

        # Assert not implemented is raised with undefined read type
        with self.assertRaises(NotImplementedError):
            reader.source['type'] = "any type"
            reader.read()
