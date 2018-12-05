import unittest

from gobimport.connector.config import get_url, ORACLE_DRIVER

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
