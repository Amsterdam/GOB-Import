from unittest import TestCase

from gobcore.exceptions import GOBException
from gobprepare.cloner.mapping.oracle_to_postgres import (
    _oracle_number_to_postgres, get_postgres_column_definition)


class TestOracleToPostgresMapping(TestCase):

    def test_oracle_number_to_postgres(self):
        cases = [
            ((None, 2, 0), 'NUMERIC(2,0)'),
            ((None, 5, 2), 'NUMERIC(5,2)'),
            ((0, None, None), 'SMALLINT'),
            ((4, None, None), 'SMALLINT'),
            ((5, None, None), 'INT'),
            ((9, None, None), 'INT'),
            ((10, None, None), 'BIGINT'),
            ((18, None, None), 'BIGINT'),
            ((19, None, None), 'NUMERIC(19)'),
            ((428, None, None), 'NUMERIC(428)'),
        ]

        for params, result in cases:
            length, precision, scale = params
            self.assertEquals(result, _oracle_number_to_postgres(length, precision, scale))

    def test_oracle_number_to_postgres_errors(self):
        cases = [
            # Only precision
            (None, 5, None),
            # Only scale
            (None, None, 5),
            # All none
            (None, None, None),
        ]

        for length, precision, scale in cases:
            with self.assertRaises(GOBException):
                _oracle_number_to_postgres(length, precision, scale)

    def test_get_postgres_column_definition_from_mapping(self):
        cases = [
            (('VARCHAR2', 5, None, None), 'VARCHAR(5)'),
            (('NCHAR', 4, None, None), 'CHAR(4)'),
            (('DATE', None, None, None), 'TIMESTAMP(0)'),
            (('TIMESTAMP WITH LOCAL TIME ZONE', None, None, None), 'TIMESTAMPTZ'),
            (('BLOB', None, None, None), 'BYTEA'),
            (('SDO_GEOMETRY', None, None, None), 'GEOMETRY'),
        ]

        for params, result in cases:
            data_type, length, precision, scale = params
            self.assertEquals(result, get_postgres_column_definition(data_type, length, precision, scale))

    def test_get_postgres_column_definition_number(self):
        self.assertEquals('NUMERIC(3,2)', get_postgres_column_definition('NUMBER', None, 3, 2))

    def test_get_postgres_column_definition_missing_column(self):
        with self.assertRaises(GOBException):
            get_postgres_column_definition("BOGUS_COLUMN", 1, 2, 3)
