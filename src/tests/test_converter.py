import unittest
from unittest import mock

from decimal import Decimal
from gobcore.model import GOBModel
from gobimport.converter import _apply_filters, _extract_references, _is_object_reference, Converter, _json_safe_value
from tests.fixtures import random_string


class TestConverter(unittest.TestCase):

    def setUp(self):
        pass

    def test_apply_filters(self):
        filters = [
            ["re.sub", "a", "b"],
            ["upper"]
        ]
        result = _apply_filters("a", filters)
        self.assertEqual(result, "B")

        filters = [
            ["upper"],
            ["re.sub", "a", "b"],
        ]
        result = _apply_filters("a", filters)
        self.assertEqual(result, "A")

    def test_is_object_reference(self):
        testcases = (
            ("this.andthat", True),
            ("JKDldjkd.jklfD", True),
            (["ajkdlf", ".", "jdkladf"], False),
            ("kdkdkdfklajdf", False),
            (".", False),
            ("kdkakdkaf.", False),
            (".kdljfakld", False),
            ("", False),
            ("kdjld_.dkdj_dkldjf", True),
        )

        for testcase, result in testcases:
            self.assertEqual(result, _is_object_reference(testcase), f"Case {testcase} should return {result}")

    def test_extract_references_bronwaarde_object_ref(self):
        row = {
            "id": random_string(),
            "name": random_string(),
            "col": random_string(),
            "ref_col": [{
                "ref_attr": random_string(),
                "other_col": random_string(),
                "another_col": random_string(),
            }, {
                "ref_attr": random_string(),
                "other_col": random_string(),
                "another_col": random_string(),
            }],
        }
        field_source = {
            "bronwaarde": "ref_col.ref_attr",
        }
        field_type = "GOB.ManyReference"
        result = _extract_references(row, field_source, field_type)
        expected_result = [{
            "bronwaarde": row["ref_col"][0]["ref_attr"],
            "ref_attr": row["ref_col"][0]["ref_attr"],
            "other_col": row["ref_col"][0]["other_col"],
            "another_col": row["ref_col"][0]["another_col"],
        }, {
            "bronwaarde": row["ref_col"][1]["ref_attr"],
            "ref_attr": row["ref_col"][1]["ref_attr"],
            "other_col": row["ref_col"][1]["other_col"],
            "another_col": row["ref_col"][1]["another_col"],
        }]
        self.assertEqual(expected_result, result)

    def test_extract_references_bronwaarde_object_two_refs(self):
        row = {
            "id": random_string(),
            "name": random_string(),
            "col": random_string(),
            "ref_col": [{
                "ref_attr": random_string(),
                "other_ref_attr": random_string(),
                "other_col": random_string(),
                "another_col": random_string(),
            }, {
                "ref_attr": random_string(),
                "other_ref_attr": random_string(),
                "other_col": random_string(),
                "another_col": random_string(),
            }],
        }
        field_source = {
            "bronwaarde": "ref_col.ref_attr",
            "bronwaarde2": "ref_col.other_ref_attr",
        }
        field_type = "GOB.ManyReference"
        result = _extract_references(row, field_source, field_type)
        expected_result = [{
            "bronwaarde": row["ref_col"][0]["ref_attr"],
            "bronwaarde2": row["ref_col"][0]["other_ref_attr"],
            "ref_attr": row["ref_col"][0]["ref_attr"],
            "other_ref_attr": row["ref_col"][0]["other_ref_attr"],
            "other_col": row["ref_col"][0]["other_col"],
            "another_col": row["ref_col"][0]["another_col"],
        }, {
            "bronwaarde": row["ref_col"][1]["ref_attr"],
            "bronwaarde2": row["ref_col"][1]["other_ref_attr"],
            "ref_attr": row["ref_col"][1]["ref_attr"],
            "other_ref_attr": row["ref_col"][1]["other_ref_attr"],
            "other_col": row["ref_col"][1]["other_col"],
            "another_col": row["ref_col"][1]["another_col"],
        }]
        self.assertEqual(expected_result, result)

    @mock.patch("gobimport.converter._json_safe_value", lambda x: 'safe ' + x)
    @mock.patch("gobimport.converter._get_value", lambda x, y: x[y])
    def test_extract_references_not_many(self):
        row = {
            "rowkey a": "val a",
            "rowkey b": "val b",
        }
        field_source = {
            "key a": "rowkey a",
            "key b": "rowkey b"
        }
        field_type = "GOB.JSON"

        expected_result = {
            'key a': 'safe val a',
            'key b': 'safe val b',
        }

        self.assertEqual(expected_result, _extract_references(row, field_source, field_type))

    def test_json_safe_value(self):
        testcases = [
            ("abc", "abc"),
            (12, 12),
            (12.2, 12.2),
            (None, None),
            (Decimal(1.5), "1.5")
        ]

        for arg, result in testcases:
            self.assertEqual(result, _json_safe_value(arg))

    @mock.patch("gobimport.converter.GOBModel", mock.MagicMock(spec=GOBModel))
    def test_convert(self):
        row = {
            "id": random_string(),
            "name": random_string(),
            "col": random_string(),
            "ref_col": [{
                "ref_attr": random_string(),
                "other_ref_attr": random_string(),
                "other_col": random_string(),
                "another_col": random_string(),
            }, {
                "ref_attr": random_string(),
                "other_ref_attr": random_string(),
                "other_col": random_string(),
                "another_col": random_string(),
            }],
        }
        converter = Converter("catalog", "entity", {
            "gob_mapping": {}
        })
        result = converter.convert(row)
        self.assertEqual(result, {"_source_id": mock.ANY})
