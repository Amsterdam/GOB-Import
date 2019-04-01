import unittest
from unittest.mock import patch

from gobcore.exceptions import GOBException
from gobimport.config import get_database_config, get_objectstore_config, get_url, DATABASE_CONFIGS, \
    OBJECTSTORE_CONFIGS, ORACLE_DRIVER


class TestInjections(unittest.TestCase):

    def setUp(self):
        pass

    def test_get_url(self):
        # Leave Oracle SID as it is
        config = {
            'drivername': ORACLE_DRIVER,
            'username': "username",
            'password': "password",
            'host': "host",
            'port': 1234,
            'database': 'SID'
        }
        self.assertEqual(str(get_url(config)), "oracle+cx_oracle://username:password@host:1234/SID")

        # Handle Oracle service name
        config = {
            'drivername': ORACLE_DRIVER,
            'username': "username",
            'password': "password",
            'host': "host",
            'port': 1234,
            'database': 'x.y.z'
        }
        self.assertEqual(str(get_url(config)), "oracle+cx_oracle://username:password@host:1234/?service_name=x.y.z")

        # Handle it only or Oracle
        config = {
            'drivername': "driver",
            'username': "username",
            'password': "password",
            'host': "host",
            'port': 1234,
            'database': 'x.y.z'
        }
        self.assertEqual(str(get_url(config)), "driver://username:password@host:1234/x.y.z")


class TestGetConfig(unittest.TestCase):

    @patch("gobimport.config.get_url")
    def test_get_database_config(self, mock_get_url):
        get_url_result = 'someurl'
        mock_get_url.return_value = get_url_result
        name = "Neuron"
        result = get_database_config(name)

        expected = DATABASE_CONFIGS[name].copy()
        expected['url'] = get_url_result
        expected['name'] = name

        self.assertEqual(expected, result)

    def test_get_nonexistend_database_config(self):
        name = 'NonExistent'

        with self.assertRaises(GOBException):
            get_database_config(name)

    def test_get_objectstore_config(self):
        name = "Basisinformatie"
        result = get_objectstore_config(name)

        expected = OBJECTSTORE_CONFIGS[name].copy()
        expected['name'] = name

        self.assertEqual(expected, result)

    def test_get_nonexistend_objectstore_config(self):
        name = 'NonExistent'

        with self.assertRaises(GOBException):
            get_objectstore_config(name)
