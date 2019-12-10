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

    def dataset(self):
        return {
            "gob_mapping": {},
            "catalogue": "test_catalogue",
            "entity": "test_entity"
        }

    def test_constructor(self):
        reader = Reader(self.source, self.app, self.dataset())
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

        reader = Reader(self.source, self.app, self.dataset())
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

        reader = Reader(self.source, self.app, self.dataset())
        reader._connection = "any connection"

        for read_type in test_read_types:
             with mock.patch(read_type[0]) as mock_read:

                 reader.source['type'] = read_type[1]

                 mock_read.return_value = []

                 reader.read()

                 mock_read.assert_called()

        # If a mode is specified the mode query extension should be read from the config
        with self.assertRaises(KeyError):
            # Non-existent mode
            reader.read(mode="some mode")

        # Existent mode
        self.source["my mode"] = "my mode"
        reader.read(mode="my mode")

        # Assert not implemented is raised with undefined read type
        with self.assertRaises(NotImplementedError):
            reader.source['type'] = "any type"
            reader.read()

    def test_set_secure_attributes(self):
        reader = Reader(self.source, self.app, self.dataset())
        mapping = {
            'a': {
                'source_mapping': 'any secure string'
            },
            'b': {
                'source_mapping': {
                    'bronwaarde': 'any secure bronwaarde'
                }
            },
            'c': {
                'source_mapping': 'any string'
            },
            'd': {
                'source_mapping': {
                    'bronwaarde': 'any bronwaarde'
                }
            },
        }
        attributes = {
            'a': {
                'type': 'GOB.SecureString'
            },
            'b': {
                'type': 'GOB.JSON',
                'secure': {
                    'bronwaarde': {
                        'type': 'GOB.SecureString',
                        'level': 5
                    }
                }
            },
            'c': {
                'type': 'GOB.String'
            },
            'd': {
                'type': 'GOB.JSON',
                'secure': {
                    'bronwaarde': {
                        'type': 'GOB.String',
                        'level': 5
                    }
                }
            }
        }
        reader.set_secure_attributes(mapping, attributes)
        self.assertEqual(reader.secure_attributes, ['any secure string', 'any secure bronwaarde'])
