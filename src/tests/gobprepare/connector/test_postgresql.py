from unittest import TestCase
from unittest.mock import patch

from gobcore.exceptions import GOBException
from gobprepare.connector.postgresql import connect_to_postgresql
from psycopg2 import OperationalError
from tests.fixtures import random_string

bogus_config = {
    "GOBDB": {
        "database": random_string(),
        "username": random_string(),
        "password": random_string(),
        "host": random_string(),
        "port": random_string(),
    },
}


@patch("gobprepare.connector.postgresql.DATABASE_CONFIGS", bogus_config)
@patch("gobprepare.connector.postgresql.psycopg2.connect")
class TestPostgresqlConnector(TestCase):

    def test_connect_to_postgresql(self, mock_connect):
        """Asserts the psycopg2.connect method is called with the correct parameters """

        connect_to_postgresql({"application": "GOBDB"})
        mock_connect.assert_called_with(
            database=bogus_config["GOBDB"]["database"],
            user=bogus_config["GOBDB"]["username"],
            password=bogus_config["GOBDB"]["password"],
            host=bogus_config["GOBDB"]["host"],
            port=bogus_config["GOBDB"]["port"],
        )

    def test_nonexisting_config(self, mock_connect):
        """Asserts that GOBException is raised when using non-existing config """

        with self.assertRaises(GOBException):
            connect_to_postgresql({"application": "NonExistent"})

    def test_psycopg2_exception(self, mock_connect):
        """Asserts that psycopg2 Error is reraised as GOBException """

        mock_connect.side_effect = OperationalError

        with self.assertRaises(GOBException):
            connect_to_postgresql({"application": "GOBDB"})
