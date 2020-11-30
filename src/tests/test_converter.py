import unittest
from unittest import mock

from decimal import Decimal
from gobcore.model import GOBModel
from gobcore.model.metadata import FIELD
from gobimport.converter import _apply_filters, _extract_references, _is_object_reference, _split_object_reference, \
                                Converter, _json_safe_value, _get_value, _clean_references, _extract_field, _goblike_row
from gobcore.exceptions import GOBException, GOBTypeException
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

        filters = [
            ["some name"],
            ["re.sub", "a", "b"],
        ]
        with self.assertRaises(GOBException):
            result = _apply_filters("a", filters)


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

    def test_split_object_reference(self):
        valid_testcase = (
            ("this.andthat", ("this", "andthat")),
        )

        for testcase, result in valid_testcase:
            self.assertEqual(result, _split_object_reference(testcase), f"Case {testcase} should return {result}")

        invalid_testcases = [
            "this",
            "this.that.andthat",
            ".",
        ]

        with self.assertRaises(GOBException):
            for testcase in invalid_testcases:
                _split_object_reference(testcase)

    def test_get_value(self):
        valid_testcase = (
            ({'row':{'field': 'value'}, 'field':'field'}, 'value'),
            ({'row':{}, 'field':'=field'}, 'field'),
            ({'row':{'field': {'nested_field': 'value'}}, 'field':'field.nested_field'}, {'nested_field': 'value'}),
        )

        for testcase, result in valid_testcase:
            self.assertEqual(result, _get_value(testcase['row'], testcase['field']), f"Case {testcase} should return {result}")

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

        field_source = {
            "bronwaarde": "=somevalue",
        }
        result = _extract_references(row, field_source, field_type)
        expected_result = [{
            "bronwaarde": 'somevalue'
        }]
        self.assertEqual(expected_result, result)

    def test_extract_references_json_force_list(self):
        row = {
            "id": random_string(),
            "name": random_string(),
            "col": random_string(),
        }
        field_source = {
            "someattr": "col",
        }
        field_type = "GOB.JSON"
        result = _extract_references(row, field_source, field_type, True)

        expected_result = [{'someattr': row['col']}]
        self.assertEqual(expected_result, result)

        # And now without force_list set to True
        result = _extract_references(row, field_source, field_type, False)
        expected_result = {'someattr': row['col']}
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

    def test_extract_references_singleref_objectref(self):
        row = {
            "id": random_string(),
            "name": random_string(),
            "ref_col": {
                "a": random_string(),
                "b": random_string(),
                "c": random_string(),
            }
        }
        field_source = {
            "bronwaarde": "ref_col.b"
        }
        field_type = "GOB.Reference"

        result = _extract_references(row, field_source, field_type)
        expected_value = {
            "a": row["ref_col"]["a"],
            "b": row["ref_col"]["b"],
            "c": row["ref_col"]["c"],
            "bronwaarde": row["ref_col"]["b"]
        }
        self.assertEqual(expected_value, result)

    def test_clean_reference(self):
        reference = {
            'bronwaarde': random_string(),
            'other_col': random_string(),
        }

        expected_result = {
            'bronwaarde': reference['bronwaarde'],
            'broninfo': {
                'other_col': reference['other_col'],
            }
        }
        result = _clean_references(reference)
        self.assertEqual(expected_result, result)


    def test_clean_reference_without_other_values(self):
        reference = {
            'bronwaarde': random_string(),
        }

        expected_result = {
            'bronwaarde': reference['bronwaarde'],
        }
        result = _clean_references(reference)
        self.assertEqual(expected_result, result)


    def test_clean_manyreference(self):
        reference = [{
            'bronwaarde': random_string(),
            'other_col': random_string(),
        },
        {
            'bronwaarde': random_string(),
            'other_col': random_string(),
        }]

        expected_result = [{
            'bronwaarde': reference[0]['bronwaarde'],
            'broninfo': {
                'other_col': reference[0]['other_col'],
            }
        },
        {
            'bronwaarde': reference[1]['bronwaarde'],
            'broninfo': {
                'other_col': reference[1]['other_col'],
            }
        }]
        result = _clean_references(reference)
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
            "gob_mapping": {},
            "source": {
                "entity_id": "any entity id"
            }
        })
        result = converter.convert(row)
        self.assertEqual(result, {"_source_id": mock.ANY})

    def test_goblike_row(self):
        entity_id_field = 'entity_id field'
        seqnr_field = 'seqnr field'
        row = {
            entity_id_field: 'any entity_id',
            seqnr_field: 'any seqnr'
        }
        self.assertEqual(_goblike_row(row, entity_id_field), {
            **row,
            FIELD.ID: row[entity_id_field]
        })
        self.assertEqual(_goblike_row(row, entity_id_field, seqnr_field), {
            **row,
            FIELD.ID: row[entity_id_field],
            FIELD.SEQNR: row[seqnr_field]
        })
        self.assertEqual(row, {
            entity_id_field: 'any entity_id',
            seqnr_field: 'any seqnr'
        })

    def test_string_split_many_reference(self):
        row = {
            's1': "abc",
            's2': "a;b;c",
        }
        field_type = 'GOB.ManyReference'
        source = {}

        source['bronwaarde'] = 's1'
        result = _extract_references(row, source, field_type)
        self.assertEqual(result, [{'bronwaarde': 'abc'}])

        source['bronwaarde'] = 's2'
        result = _extract_references(row, source, field_type)
        self.assertEqual(result, [{'bronwaarde': 'a;b;c'}])

        source['format'] = {}

        source['bronwaarde'] = 's1'
        result = _extract_references(row, source, field_type)
        self.assertEqual(result, [{'bronwaarde': 'abc'}])

        source['bronwaarde'] = 's2'
        result = _extract_references(row, source, field_type)
        self.assertEqual(result, [{'bronwaarde': 'a;b;c'}])

        source['format'] = {'split': ";"}

        source['bronwaarde'] = 's1'
        result = _extract_references(row, source, field_type)
        self.assertEqual(result, [{'bronwaarde': 'abc'}])

        source['bronwaarde'] = 's2'
        result = _extract_references(row, source, field_type)
        self.assertEqual(result, [{'bronwaarde': 'a'}, {'bronwaarde': 'b'}, {'bronwaarde': 'c'}])

        source['bronwaarde'] = 's2'
        row['s2'] = 'aap;noot;mies;mies;noot;aap'
        result = _extract_references(row, source, field_type)
        self.assertEqual(result, [{'bronwaarde': 'aap'}, {'bronwaarde': 'mies'}, {'bronwaarde': 'noot'}])

    @mock.patch('gobimport.converter.logger')
    @mock.patch('gobimport.converter.get_gob_type_from_info')
    @mock.patch('gobimport.converter._get_value')
    def test_extract_field(self, mock_get_value, mock_get_gob_type_from_info, mock_logger):
        row = {
            '_id': '12345',
        }
        field = 'f'
        metadata = {
            'source_mapping': 'any mapping'
        }
        typeinfo = {
            'type': 'any type'
        }
        # Implementation test of extract field
        mock_gob_type = mock.MagicMock()
        mock_get_gob_type_from_info.return_value = mock_gob_type
        result = _extract_field(row, field, metadata, typeinfo)
        self.assertEqual(result, mock_gob_type.from_value_secure.return_value)
        mock_get_value.assert_called_with(row, metadata['source_mapping'])
        mock_gob_type.from_value_secure.assert_called_with(mock_get_value.return_value, typeinfo)

        # Behaviour test, if GOB Type conversion fails a data error should be reported
        # And a GOB Type None value should be returned
        mock_gob_type.from_value_secure.side_effect = [GOBTypeException(), None]
        result = _extract_field(row, field, metadata, typeinfo)
        self.assertEqual(result, None)

        # Assert error is generated
        mock_logger.error.assert_called_once()
