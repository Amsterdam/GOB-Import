from unittest import TestCase, mock

from gobimport.reader import Reader, ImportMode


@mock.patch('gobimport.reader.logger', mock.MagicMock())
class TestReader(TestCase):

    def setUp(self):
        self.source = {
            "type": "any type",
            "application": "any application",
            "query": "any query",
            "config": {
                "filename": "any filename",  # only used for file
            },
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
        self.assertEqual(reader.datastore, None)
        self.assertEqual(ImportMode.FULL, reader.mode)

        reader = Reader(self.source, self.app, self.dataset(), 'other mode')
        self.assertEqual('other mode', reader.mode)

    @mock.patch("gobimport.reader.get_datastore_config")
    @mock.patch("gobimport.reader.DatastoreFactory")
    def test_connect(self, mock_datastore_factory, mock_datastore_config):
        reader = Reader(self.source, self.app, self.dataset())

        # 1. Should use application config to connect
        reader.source = {
            'application_config': 'the application config',
            'application': 'the application',
            'read_config': {'read': 'config'},
        }
        reader.mode = 'the mode'

        reader.connect()

        self.assertEqual(mock_datastore_factory.get_datastore.return_value, reader.datastore)
        reader.datastore.connect.assert_called_once()

        mock_datastore_factory.get_datastore.assert_called_with('the application config', {
            'read': 'config',
            'mode': 'the mode'
        })
        mock_datastore_config.assert_not_called()

        # 2. Application defined, no application_config defined. Should get application config
        mock_datastore_factory.reset_mock()
        reader.source = {
            'application': 'the application'
        }
        reader.datastore = None

        reader.connect()
        self.assertEqual(mock_datastore_factory.get_datastore.return_value, reader.datastore)
        reader.datastore.connect.assert_called_once()

        mock_datastore_factory.get_datastore.assert_called_with(
            mock_datastore_config.return_value, {'mode': 'the mode'})
        mock_datastore_config.assert_called_with('the application')

    def test_read(self):
        reader = Reader({'query': ['a', 'b', 'c']}, self.app, self.dataset())
        reader.datastore = mock.MagicMock()
        reader._maybe_protect_rows = mock.MagicMock()
        query_kwargs = {'arraysize': 2000, 'name': 'import_cursor', 'withhold': True}

        result = reader.read()
        self.assertEqual(reader._maybe_protect_rows.return_value, result)
        reader._maybe_protect_rows.assert_called_with(reader.datastore.query.return_value)
        reader.datastore.query.assert_called_with('a\nb\nc', **query_kwargs)

        reader.source = {'query': ['a', 'b', 'c'], 'recent': ['d', 'e']}
        reader.mode = ImportMode.RECENT
        reader.read()
        reader.datastore.query.assert_called_with('a\nb\nc\nd\ne', **query_kwargs)

        reader.source = {'query': ['a', 'b', 'c'], 'delete': ['d', 'e']}
        reader.mode = ImportMode.UPDATE
        with self.assertRaises(KeyError):
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

    @mock.patch("gobimport.reader.read_protect", lambda x: 'read_protected(' + x + ')')
    def test_protect_row(self):
        reader = Reader(self.source, self.app, self.dataset())
        reader.secure_attributes = ['attrB']
        row = {'attrA': 'valA', 'attrB': 'valB'}

        self.assertEqual({
            'attrA': 'valA',
            'attrB': 'read_protected(valB)',
        }, reader._protect_row(row))

    def test_query(self):
        reader = Reader(self.source, self.app, self.dataset())
        reader._protect_row = lambda x: 'protected(' + x + ')'
        reader.secure_attributes = []
        query = iter(['a', 'b'])

        self.assertEqual(['a', 'b'], list(reader._maybe_protect_rows(query)))

        query = iter(['a', 'b'])
        reader.secure_attributes = ['not', 'empty']
        self.assertEqual([
            'protected(a)',
            'protected(b)',
        ], list(reader._maybe_protect_rows(query)))

    def test_disconnect(self):
        with Reader(self.source, self.app, self.dataset()) as reader:
            reader.datastore = mock.MagicMock()

        reader.datastore.disconnect.assert_called_once()
