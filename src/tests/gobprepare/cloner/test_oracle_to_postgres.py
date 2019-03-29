from unittest import TestCase
from unittest.mock import MagicMock, call, patch

from gobcore.exceptions import GOBEmptyResultException
from gobprepare.cloner.oracle_to_postgres import OracleToPostgresCloner
from tests.fixtures import random_string


class TestOracleToPostgresCloner(TestCase):

    def setUp(self):
        self.oracle_connection_mock = MagicMock()
        self.postgres_connection_mock = MagicMock()
        self.src_schema = random_string()
        self.dst_schema = random_string()

        self.cloner = OracleToPostgresCloner(
            self.oracle_connection_mock,
            self.src_schema,
            self.postgres_connection_mock,
            self.dst_schema,
            {}
        )

    def test_init_no_mask_columns_or_ignore_tables(self):
        # Test empty dict
        cloner = OracleToPostgresCloner(self.oracle_connection_mock, self.src_schema, self.postgres_connection_mock,
                                        self.dst_schema, {})

        self.assertEquals({}, cloner._mask_columns)
        self.assertEquals([], cloner._ignore_tables)

        # Test None
        cloner = OracleToPostgresCloner(self.oracle_connection_mock, self.src_schema, self.postgres_connection_mock,
                                        self.dst_schema, None)
        self.assertEquals({}, cloner._mask_columns)
        self.assertEquals([], cloner._ignore_tables)

    def test_init_with_mask_columns_and_ignore_tables(self):
        config = {
            "mask": {
                "table_name": {
                    "column_a": "mask_a",
                    "column_b": "mask_b",
                },
            },
            "ignore": [
                "table_a",
                "table_b",
            ]
        }
        cloner = OracleToPostgresCloner(self.oracle_connection_mock, self.src_schema, self.postgres_connection_mock,
                                        self.dst_schema, config)

        self.assertEquals(config["mask"], cloner._mask_columns)
        self.assertEquals(config["ignore"], cloner._ignore_tables)

    @patch("gobprepare.cloner.oracle_to_postgres.read_from_oracle")
    def test_read_source_table_names(self, mock_read_from_oracle):
        mock_read_from_oracle.return_value = [{'table_name': 'tableA'}, {'table_name': 'tableB'}]

        self.assertEqual(['tableA', 'tableB'], self.cloner._read_source_table_names())

        mock_read_from_oracle.assert_called_with(
            self.oracle_connection_mock,
            [f"SELECT table_name FROM all_tables WHERE owner='{self.src_schema}' ORDER BY table_name"]
        )

    @patch("gobprepare.cloner.oracle_to_postgres.read_from_oracle")
    def test_read_source_table_names_ignore_tables(self, mock_read_from_oracle):
        self.cloner._ignore_tables = ['table_1', 'table_2']
        mock_read_from_oracle.return_value = [{'table_name': 'tableA'}, {'table_name': 'tableB'}]
        self.assertEqual(['tableA', 'tableB'], self.cloner._read_source_table_names())

        mock_read_from_oracle.assert_called_with(
            self.oracle_connection_mock,
            [f"SELECT table_name FROM all_tables WHERE owner='{self.src_schema}' AND "
             f"table_name NOT IN ('table_1','table_2') ORDER BY table_name"]
        )

    @patch("gobprepare.cloner.oracle_to_postgres.read_from_oracle")
    @patch("gobprepare.cloner.oracle_to_postgres.get_postgres_column_definition")
    def test_get_source_table_definition(self, mock_get_postgres_column_definition, mock_read_from_oracle):
        mock_get_postgres_column_definition.return_value = "COLUMNDEF()"
        mock_read_from_oracle.return_value = [
            {
                "column_name": "columnA",
                "data_type": "some_int",
                "data_length": 8,
                "data_precision": None,
                "data_scale": None
            },
            {
                "column_name": "columnB",
                "data_type": "some_number",
                "data_length": None,
                "data_precision": 5,
                "data_scale": 2
            },
        ]
        expected_result = [
            ("columnA", "COLUMNDEF()"),
            ("columnB", "COLUMNDEF()"),
        ]
        table_name = "some_table_name"

        result = self.cloner._get_source_table_definition(table_name)
        self.assertEqual(expected_result, result)

        mock_get_postgres_column_definition.assert_any_call("some_int", 8, None, None)
        mock_get_postgres_column_definition.assert_any_call("some_number", None, 5, 2)
        mock_read_from_oracle.assert_called_with(
            self.oracle_connection_mock,
            [f"SELECT column_name, data_type, data_length, data_precision, data_scale FROM all_tab_columns WHERE "
             f"owner='{self.src_schema}' AND table_name='{table_name}'"],
        )

    @patch("gobprepare.cloner.oracle_to_postgres.logger")
    @patch("gobprepare.cloner.oracle_to_postgres.drop_schema")
    @patch("gobprepare.cloner.oracle_to_postgres.create_schema")
    def test_prepare_destination_database(self, mock_create_schema, mock_drop_schema, mock_logger):
        self.cloner._get_destination_schema_definition = MagicMock(
            return_value=["tabledef_a", "tabledef_b", "tabledef_c"]
        )
        self.cloner._create_destination_table = MagicMock()

        self.cloner._prepare_destination_database()

        mock_drop_schema.assert_called_with(self.postgres_connection_mock, self.dst_schema)
        mock_create_schema.assert_called_with(self.postgres_connection_mock, self.dst_schema)
        self.cloner._create_destination_table.assert_has_calls([
            call("tabledef_a"), call("tabledef_b"), call("tabledef_c")
        ])
        mock_logger.info.assert_called_once()

    @patch("gobprepare.cloner.oracle_to_postgres.drop_table")
    @patch("gobprepare.cloner.oracle_to_postgres.execute_postgresql_query")
    def test_create_destination_table(self, mock_execute, mock_drop_table):
        table_definition = ("tablename", [('id', 'INT'), ('first_name', 'VARCHAR(20)'), ('last_name', 'VARCHAR(20)')])
        self.cloner._create_destination_table(table_definition)

        mock_drop_table.assert_called_with(self.postgres_connection_mock, f"{self.dst_schema}.tablename")
        mock_execute.assert_called_with(
            self.postgres_connection_mock,
            f"CREATE TABLE {self.dst_schema}.tablename (id INT NULL,first_name VARCHAR(20) NULL,last_name VARCHAR(20) NULL)"
        )

    def test_get_destination_schema_definition(self):
        self.cloner._read_source_table_names = MagicMock(return_value=["table_a", "table_b"])
        self.cloner._get_source_table_definition = MagicMock(return_value="some table definition")

        expected_result = [("table_a", "some table definition"), ("table_b", "some table definition")]

        # Call twice to trigger both paths
        self.assertEqual(expected_result, self.cloner._get_destination_schema_definition())
        self.assertEqual(expected_result, self.cloner._get_destination_schema_definition())
        self.cloner._get_source_table_definition.assert_has_calls([call("table_a"), call("table_b")])
        self.cloner._read_source_table_names.assert_called_once()

    def test_copy_data(self):
        self.cloner._get_destination_schema_definition = MagicMock(return_value=[
            ("tabledef_a", []), ("tabledef_b", []), ("tabledef_c", [])
        ])
        self.cloner._copy_table_data = MagicMock(return_value=4)
        self.assertEqual(12, self.cloner._copy_data())
        self.cloner._copy_table_data.has_calls([
            call("tabledef_a"),
            call("tabledef_b"),
            call("tabledef_c"),
        ])

    @patch("gobprepare.cloner.oracle_to_postgres.logger")
    @patch("gobprepare.cloner.oracle_to_postgres.read_from_oracle")
    def test_copy_table_data(self, mock_read_from_oracle, mock_logger):
        # Mock result as list of 0's
        mock_read_from_oracle.side_effect = [self.cloner.READ_BATCH_SIZE * [0], (self.cloner.READ_BATCH_SIZE - 1) * [0]]

        self.cloner._get_select_list_for_table_definition = MagicMock(return_value="colA, colB, colC")
        self.cloner._insert_rows = MagicMock()
        self.cloner._mask_columns = MagicMock()

        self.assertEqual(self.cloner.READ_BATCH_SIZE * 2 - 1, self.cloner._copy_table_data(("tableName", [])))

        mock_read_from_oracle.assert_has_calls([
            call(self.oracle_connection_mock, [f"SELECT colA, colB, colC FROM {self.src_schema}.tableName OFFSET 0 ROWS FETCH FIRST {self.cloner.READ_BATCH_SIZE} ROWS ONLY"]),
            call(self.oracle_connection_mock, [f"SELECT colA, colB, colC FROM {self.src_schema}.tableName OFFSET {self.cloner.READ_BATCH_SIZE} ROWS FETCH FIRST {self.cloner.READ_BATCH_SIZE} ROWS ONLY"]),
        ])

        mock_logger.info.assert_called_once()
        self.cloner._mask_columns.assert_not_called()

    @patch("gobprepare.cloner.oracle_to_postgres.logger")
    @patch("gobprepare.cloner.oracle_to_postgres.read_from_oracle")
    def test_copy_table_data_with_mask(self, mock_read_from_oracle, mock_logger):
        table_definition = ("table_name", [("id", "INT"), ("name", "VARCHAR")])
        self.cloner._get_select_list_for_table_definition = MagicMock(return_value="id, name")
        self.cloner._mask_columns = {
            "table_name": {
                "name": "***"
            }
        }
        self.cloner._insert_rows = MagicMock()
        self.cloner._mask_rows = MagicMock()
        self.cloner._copy_table_data(table_definition)
        self.cloner._mask_rows.assert_called_once()

    def test_mask_rows(self):
        self.cloner._mask_columns = {
            "table_name": {
                "name": "***"
            }
        }
        row_data = [
            {"id": 4, "name": "Sheldon"},
            {"id": 5, "name": "Leonard"},
        ]
        expected_result = [
            {"id": 4, "name": "***"},
            {"id": 5, "name": "***"},
        ]
        self.assertEqual(expected_result, self.cloner._mask_rows("table_name", row_data))

    def test_mask_rows_nothing_to_mask(self):
        self.cloner._mask_columns = {
            "table_name": {
                "name": "***"
            }
        }
        row_data = [
            {"id": 4, "name": "Sheldon"},
            {"id": 5, "name": "Leonard"},
        ]
        self.assertEqual(row_data, self.cloner._mask_rows("table_name_not_in_mask_columns", row_data))


    @patch("gobprepare.cloner.oracle_to_postgres.logger")
    @patch("gobprepare.cloner.oracle_to_postgres.read_from_oracle")
    def test_copy_table_data_empty_table(self, mock_read_from_oracle, mock_logger):
        mock_read_from_oracle.side_effect = GOBEmptyResultException
        self.cloner._get_select_list_for_table_definition = MagicMock(return_value="colA, colB, colC")
        self.cloner._insert_rows = MagicMock()

        self.assertEqual(0, self.cloner._copy_table_data(("tableName", [])))
        mock_logger.info.assert_called_once()

    @patch("gobprepare.cloner.oracle_to_postgres.write_rows_to_postgresql")
    def test_insert_rows(self, mock_write):
        self.cloner.WRITE_BATCH_SIZE = 2

        table_definition = ("table_name", [
            ("id", "INT"),
            ("name", "VARCHAR(20)"),
        ])
        row_data = [
            {"name": "Sheldon", "id": 3},
            {"name": "Leonard", "id": 4},
            {"name": "Amy", "id": 7},
            {"name": "Rajesh", "id": 1},
            {"name": "Howard", "id": 9},
            {"name": "Penny", "id": 5},
            {"name": "Kripke", "id": 2},
        ]
        self.cloner._insert_rows(table_definition, row_data)

        full_table_name = f"{self.dst_schema}.table_name"
        # The rows should be divided in chunks of size WRITE_BATCH_SIZE and the values should be in order according to
        # table_definition
        mock_write.assert_has_calls([
            call(self.postgres_connection_mock, full_table_name, [[3, "Sheldon"], [4, "Leonard"]]),
            call(self.postgres_connection_mock, full_table_name, [[7, "Amy"], [1, "Rajesh"]]),
            call(self.postgres_connection_mock, full_table_name, [[9, "Howard"], [5, "Penny"]]),
            call(self.postgres_connection_mock, full_table_name, [[2, "Kripke"]]),
        ])

    def test_get_select_list_for_table_definition(self):
        table_definition = ("table_name", [
            ("id", "INT"),
            ("first_name", "VARCHAR(20)"),
            ("last_name", "VARCHAR(21)"),
        ])

        self.cloner._get_select_expr = MagicMock()
        self.cloner._get_select_expr.side_effect = ["COLUMN_A", "COLUMN_B", "COLUMN_C"]

        expected_result = "COLUMN_A,COLUMN_B,COLUMN_C"
        result = self.cloner._get_select_list_for_table_definition(table_definition)
        self.assertEqual(expected_result, result)
        self.cloner._get_select_expr.assert_has_calls([
            call(("id", "INT")),
            call(("first_name", "VARCHAR(20)")),
            call(("last_name", "VARCHAR(21)")),
        ])

    def test_get_select_expr(self):
        test_cases = [
            (("id", "INT"), "id"),
            (("first_name", "VARCHAR(20)"), "first_name"),
            (("location", "GEOMETRY"), "SDO_UTIL.TO_WKTGEOMETRY(location) AS location"),
            (("blobby", "BYTEA"), "DBMS_LOB.SUBSTR(blobby) AS blobby"),
        ]

        for arg, result in test_cases:
            self.assertEqual(result, self.cloner._get_select_expr(arg))

    @patch("gobprepare.cloner.oracle_to_postgres.logger")
    def test_clone(self, mock_logger):
        self.cloner._prepare_destination_database = MagicMock()
        self.cloner._copy_data = MagicMock(return_value=824802)

        self.cloner.clone()
        self.cloner._prepare_destination_database.assert_called_once()
        self.cloner._copy_data.assert_called_once()

        # Assert the total number of rows is logged
        args = mock_logger.info.call_args[0]
        self.assertTrue("824802" in args[0])
