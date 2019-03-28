"""
Contains the OracleToPostgresCloner class, which contains the logic to clone an Oracle database schema to a Postgres
database.
"""

from math import ceil
from typing import Dict, List, Tuple

from gobcore.exceptions import GOBEmptyResultException
from gobcore.logging.logger import logger
from gobimport.reader.oracle import read_from_oracle
from gobprepare.cloner.mapping.oracle_to_postgres import \
    get_postgres_column_definition
from gobprepare.writer.postgresql import (create_schema, drop_schema,
                                          drop_table, execute_postgresql_query,
                                          write_rows_to_postgresql)


class OracleToPostgresCloner():
    READ_BATCH_SIZE = 100000
    WRITE_BATCH_SIZE = 100000

    schema_definition = None
    _mask_columns = {}
    _ignore_tables = []

    def __init__(self, oracle_connection, src_schema: str, postgres_connection, dst_schema: str, config: dict):
        """
        :param oracle_connection:
        :param src_schema:
        :param postgres_connection:
        :param dst_schema:
        :param config:
        """
        self._src_connection = oracle_connection
        self._src_schema = src_schema
        self._dst_connection = postgres_connection
        self._dst_schema = dst_schema

        if config is not None:
            self._mask_columns = config['mask'] if 'mask' in config else {}
            self._ignore_tables = config['ignore'] if 'ignore' in config else []

    def _read_source_table_names(self) -> list:
        """Returns a list of table names present in given schema

        :return:
        """
        def quote_string(string):
            return f"'{string}'"

        if len(self._ignore_tables) > 0:
            ignore = f" AND table_name NOT IN ({','.join([quote_string(table) for table in self._ignore_tables])})"
        else:
            ignore = ""

        query = f"SELECT table_name FROM all_tables WHERE owner='{self._src_schema}'{ignore} ORDER BY table_name"
        table_names = read_from_oracle(self._src_connection, [query])
        return [row['table_name'] for row in table_names]

    def _get_source_table_definition(self, table: str) -> List[Tuple[str, str]]:
        """
        Returns the column definitions for given table. The result is a list of 2-tuples, where each
        tuple represents a column. The first element of the tuple contains the column name, the
        second element the string representation of the column definition (as you would use in a
        create query).

        For example: [(first_name, VARCHAR(20)), (age, SMALLINT), (birthday, DATE)]

        :param table:
        :return:
        """
        query = f"SELECT column_name, data_type, data_length, data_precision, data_scale " \
            f"FROM all_tab_columns WHERE owner='{self._src_schema}' AND table_name='{table}'"
        columns = read_from_oracle(self._src_connection, [query])

        table_definition = [
            (
                column['column_name'],
                get_postgres_column_definition(
                    column["data_type"],
                    column["data_length"],
                    column["data_precision"],
                    column["data_scale"],
                )
            ) for column in columns
        ]
        return table_definition

    def _prepare_destination_database(self) -> None:
        """Creates the schema in the destination database. Removes the existing schema if exists.

        :return:
        """
        schema_definition = self._get_destination_schema_definition()
        drop_schema(self._dst_connection, self._dst_schema)
        create_schema(self._dst_connection, self._dst_schema)

        for table_definition in schema_definition:
            self._create_destination_table(table_definition)

        logger.info(f"Destination database schema {self._dst_schema} with tables created")

    def _create_destination_table(self, table_definition: Tuple[str, List]) -> None:
        """
        Creates table in destination database based on table_definition. See _get_table_definition
        for the table_definition format.

        :param table_definition:
        :return:
        """
        table_name, table_columns = table_definition
        # Create column definitions
        columns = ','.join([f"{cname} {ctype} NULL" for cname, ctype in table_columns])
        create_query = f"CREATE TABLE {self._dst_schema}.{table_name} ({columns})"

        drop_table(self._dst_connection, f"{self._dst_schema}.{table_name}")
        execute_postgresql_query(self._dst_connection, create_query)

    def _get_destination_schema_definition(self) -> List[Tuple[str, List[Tuple[str, str]]]]:
        """
        Returns a (very simple) schema definition, containing only the column data types for each
        table in the schema.

        The result is a list of tuples (table_name, table_definition), where the table_definition is
        of the form as defined in _get_table_definition.

        :param connection:
        :param schema:
        :return:
        """

        if not self.schema_definition:
            table_names = self._read_source_table_names()
            self.schema_definition = [
                (table_name, self._get_source_table_definition(table_name))
                for table_name in table_names
            ]

        return self.schema_definition

    def _copy_data(self) -> int:
        """
        Copies data from source database to destination database

        :return:
        """
        rows_copied = 0
        for table_definition in self._get_destination_schema_definition():
            rows_copied += self._copy_table_data(table_definition)

        return rows_copied

    def _copy_table_data(self, table_definition: Tuple[str, List]) -> int:
        """
        Copies table data from source database to destination database

        :param table_definition:
        :return:
        """
        table_name, _ = table_definition
        select_list = self._get_select_list_for_table_definition(table_definition)
        full_table_name = f"{self._src_schema}.{table_name}"
        cnt = 0
        mask = table_name in self._mask_columns

        while True:
            query = f"SELECT {select_list} FROM {full_table_name} " \
                f"OFFSET {cnt * self.READ_BATCH_SIZE} ROWS FETCH FIRST {self.READ_BATCH_SIZE} ROWS ONLY"

            try:
                results = read_from_oracle(self._src_connection, [query])
            except GOBEmptyResultException:
                results = []

            if mask:
                results = self._mask_rows(table_name, results)
            self._insert_rows(table_definition, results)

            if len(results) < self.READ_BATCH_SIZE:
                # We're done
                total_cnt = cnt * self.READ_BATCH_SIZE + len(results)
                logger.info(f"Written {total_cnt} rows to destination table {full_table_name}")
                return total_cnt
            cnt += 1

    def _mask_rows(self, table_name: str, row_data: List[Dict]) -> List[Dict]:
        """
        Masks the provided rows if a mask is defined in the config.

        :param table_name:
        :param row_data:
        :return:
        """
        if table_name not in self._mask_columns:
            return row_data

        for row in row_data:
            row.update(self._mask_columns[table_name])
        return row_data

    def _insert_rows(self, table_definition: Tuple[str, List], row_data: List[Dict]) -> None:
        """
        Inserts row_data into table. Input is a list of dicts with column: value pairs.

        Data is inserted in chunks of WRITE_BATCH_SIZE.

        :param table_definition:
        :param row_data:
        :return:
        """
        table_name, table_columns = table_definition
        full_table_name = f"{self._dst_schema}.{table_name}"
        # Divide rows in chunks of size WRITE_BATCH_SIZE
        chunks = [row_data[i * self.WRITE_BATCH_SIZE:i * self.WRITE_BATCH_SIZE + self.WRITE_BATCH_SIZE]
                  for i in range(ceil(len(row_data) / self.WRITE_BATCH_SIZE))]

        for chunk in chunks:
            # Input rows are dicts. Make sure values are in column order
            values = [[row[a.lower()] for (a, b) in table_columns] for row in chunk]
            write_rows_to_postgresql(self._dst_connection, full_table_name, values)

    def _get_select_list_for_table_definition(self, table_definition: Tuple[str, List]) -> str:
        """
        Returns the select list to read all columns from table.

        :param table_definition:
        :return:
        """
        return ','.join([self._get_select_expr(column_def) for column_def in table_definition[1]])

    def _get_select_expr(self, column_definition: Tuple[str, str]) -> str:
        """
        Returns the select expression to read column data from the source database in a format that
        is understood by the destination database.
        For most columns this will be the column name, but some Oracle columns need a transformation
        on selection to be understood by Postgres.

        :param column_definition:
        :return:
        """
        column_name, column_type = column_definition

        if column_type == 'GEOMETRY':
            return f'SDO_UTIL.TO_WKTGEOMETRY({column_name}) AS {column_name}'
        if column_type == 'BYTEA':
            return f'DBMS_LOB.SUBSTR({column_name}) AS {column_name}'
        # No transform function needed. Just select the column by name
        return column_name

    def clone(self):
        """
        Entry method. Copies the source Oracle database to the destination Postgres database.

        :return:
        """
        self._prepare_destination_database()
        logger.info(f"Start copying data from {self._src_schema} to {self._dst_schema}")
        rows_copied = self._copy_data()
        logger.info(f"Done copying {rows_copied} rows from {self._src_schema} to {self._dst_schema}.")
