"""Data converion logic

This module contains the logic to convert input data to GOB typed data

Todo:
    The current logic is bound to CSV files (especially the pandas.isnull logic to test for null valus)

"""
import re

from decimal import Decimal
from gobcore.typesystem import get_value, get_gob_type_from_info
from gobcore.model import GOBModel
from gobcore.model.metadata import FIELD
from gobcore.exceptions import GOBException, GOBTypeException
from gobcore.logging.logger import logger


class Converter:

    def __init__(self, catalog_name, entity_name, input_spec):
        self.gob_model = GOBModel()
        collection = self.gob_model.get_collection(catalog_name, entity_name)

        self.input_spec = input_spec
        self.mapping = input_spec['gob_mapping']
        self.fields = collection['all_fields']

        # Get the fieldnames for the id and seqnr fields
        self.entity_id = input_spec['source']['entity_id']
        self.seqnr = self.mapping.get(FIELD.SEQNR, {}).get('source_mapping')

        # Extract the fields that have a source mapping defined
        self.extract_fields = [field for field, meta in self.mapping.items() if 'source_mapping' in meta]

    def convert(self, row):
        """
        Convert the given data using the definitions in the dataset

        :param row: data in external format
        :return: entity in GOB format
        """

        # extract source fields into entity
        entity = {field: _extract_field(row,
                                        field,
                                        self.mapping[field],
                                        self.fields[field],
                                        self.entity_id,
                                        self.seqnr) for field in self.extract_fields}

        # Convert GOBTypes to python objects
        entity = get_value(entity)

        # add explicit source id, as string, to entity
        entity['_source_id'] = self.gob_model.get_source_id(entity=row, input_spec=self.input_spec)

        return entity


def _apply_filters(raw_value, filters):
    value = raw_value
    for filter in filters:
        name = filter[0]
        args = filter[1:]
        if name == "re.sub":
            value = re.sub(args[0], args[1], value)
        elif name == "upper":
            value = value.upper()
        else:
            raise GOBException(f"Unknown function {name}")
    return value


def _is_literal(field):
    """
    Tells if a field contains a literal value

    Literal values start with '='

    :param field: field name
    :return: True if field is a literal value
    """
    return field[0] == "="


def _is_object_reference(field: str):
    """
    Returns whether the field references an object attribute. Used when the input value is a JSON column with
    a list of objects and we are referencing an attribute of these objects.

    Example: A JSON column with name 'json_column', with value
    [{'attribute': 'referenced_value'}, {'attribute': 'referenced_value2'}]
    The object reference in this case would be 'json_column.attribute'

    :param field:
    :return:
    """
    return isinstance(field, str) and re.search(r"^[a-z_]+\.[a-z_]+$", field, flags=re.I) is not None


def _split_object_reference(field: str):
    """
    Splits the object reference in the source column and attribute name

    :param field:
    :return:
    """
    try:
        source, attr = field.split(".")
        return source, attr
    except ValueError:
        raise GOBException("Object reference should contain exactly one dot (.)")


def _literal_value(field):
    """
    Returns the literal value of a literal field

    Example: '=geometrie' returns 'geometrie'

    :param field: field name
    :return: the literal value of the field
    """
    return field[1:]


def _get_value(row, field):
    """
    Gets the value for a field in a row

    :param row: row of fields
    :param field: field name
    :return: the value of the specified field in the specified row
    """
    if _is_literal(field):
        # Literal value
        return _literal_value(field)
    elif _is_object_reference(field):
        column, _ = _split_object_reference(field)
        return row[column]
    else:
        # Source value
        return row[field]


def _json_safe_value(value):
    """
    Transforms value to a type that is safe for JSON serialisation.

    :param value:
    :return:
    """
    if isinstance(value, Decimal):
        return str(value)
    return value


def _extract_references(row, field_source, field_type):   # noqa: C901
    # If we can expect multiple references create an array of dicts
    FORMAT = "format"  # Optional parameter for ManyReference single string values
    if field_type == 'GOB.ManyReference':
        value = []
        # For each attribute in the source mapping, loop through all values and add them to the correct dict
        for attribute, source_mapping in field_source.items():
            if attribute in [FORMAT]:
                # Do not process format specifications
                continue
            source_value = _get_value(row, source_mapping)
            if _is_literal(source_mapping):
                value.append({attribute: source_value})
            elif _is_object_reference(source_mapping) and source_value:
                column, attr = _split_object_reference(source_mapping)

                for idx, v in enumerate(source_value):
                    if not isinstance(v, dict):
                        raise GOBException("References should be dicts when referencing by attribute")
                    # Merge referenced value with existing values
                    try:
                        value[idx].update({attribute: v[attr]})
                    except IndexError:
                        value.append({attribute: v[attr], **v})
            elif isinstance(source_value, str):
                # Accept a single string as Many Reference
                # If a format has been specified then apply the format
                # Currently only the split format is recognized
                format = field_source.get(FORMAT, {}).get('split')
                source_values = source_value.split(format)  # [source_value] if format is None
                for v in sorted(list(set(source_values))):
                    # unique values
                    value.append({attribute: v})
            elif source_value:
                for idx, v in enumerate(source_value):
                    # Try to update the dictionary with the attribute and value or create a new dict
                    try:
                        value[idx].update({attribute: v})
                    except IndexError:
                        value.append({attribute: v})
    else:
        value = {}

        for attribute, source_mapping in field_source.items():
            source_value = _json_safe_value(_get_value(row, source_mapping))

            if _is_object_reference(source_mapping) and source_value:
                if not value:
                    value = {**source_value}

                column, attr = _split_object_reference(source_mapping)

                if not isinstance(source_value, dict):
                    raise GOBException("References should be dicts when referencing by attribute")

                value[attribute] = source_value[attr]
            else:
                value[attribute] = source_value

    return value


def _clean_references(value):
    """
    Cleans the references to return a dict with bronwaarde and broninfo

    :param value: The reference or manyreference
    :return: the cleaned reference
    """
    if isinstance(value, list):
        cleaned_value = []
        for v in value:
            cleaned_value.append(_extract_source_info(v))
    else:
        cleaned_value = _extract_source_info(value)
    return cleaned_value


def _extract_source_info(value):
    """
    Extracts broninfo from the references

    :param value: The reference
    :return: the cleaned reference
    """
    # Move all attributes to source info if it's not the source value
    source_value = {FIELD.SOURCE_VALUE: value[FIELD.SOURCE_VALUE]} if FIELD.SOURCE_VALUE in value else {}
    source_info = {k: v for k, v in value.items() if k != FIELD.SOURCE_VALUE}

    # Only add broninfo if there are additional fields
    if len(source_info.keys()) == 0:
        return {**source_value}
    else:
        return {
            **source_value,
            FIELD.SOURCE_INFO: source_info
        }


def _extract_field(row, field, metadata, typeinfo, entity_id_field=None, seqnr_field=None):
    """
    Extract a field from a row given the corresponding metadata

    :param row: the data row
    :param metadata: the mapping definition
    :param typeinfo: the GOB model info
    :return: the string value of a field specified by the field's metadata, based on the values in row
    """
    field_type = typeinfo['type']
    field_source = metadata['source_mapping']

    gob_type = get_gob_type_from_info(typeinfo)

    kwargs = {k: v for k, v in metadata.items() if k not in ['type', 'source_mapping', 'filters']}

    if isinstance(field_source, dict):
        value = _extract_references(row, field_source, field_type)
    else:
        value = _get_value(row, field_source)

    # Clean all references
    if field_type in ('GOB.Reference', 'GOB.ManyReference'):
        value = _clean_references(value)

    value = _apply_field_filters(metadata, value)

    try:
        return gob_type.from_value_secure(value, typeinfo, **kwargs)
    except GOBTypeException:
        # Convert the raw source row into a GOB-like row
        report_row = _goblike_row(row, entity_id_field, seqnr_field)
        report_row[field] = value

        id = report_row[FIELD.ID] + (f".{report_row[FIELD.SEQNR]}" if seqnr_field else "")
        logger.error(f"Error importing object with id {id}. Can't extract value for field {field}")
        return gob_type.from_value_secure(None, typeinfo, **kwargs)


def _goblike_row(row, entity_id_field, seqnr_field=None):
    """
    The row is still in the source format
    Construct a "GOB-row" to report the issue

    :param entity_id_field:
    :param row:
    :param seqnr_field:
    :return:
    """
    gobrow = dict(row)
    if entity_id_field:
        gobrow[FIELD.ID] = row[entity_id_field]
    if seqnr_field:
        gobrow[FIELD.SEQNR] = row[seqnr_field]
    return gobrow


def _apply_field_filters(metadata, value):
    if "filters" in metadata:
        # If we are dealing with a dict, apply filters to the correct attribute
        if isinstance(metadata['filters'], dict):
            for attribute, filters in metadata['filters'].items():
                value[attribute] = _apply_filters(value[attribute], filters)
        else:
            # Apply any filters to the raw value
            value = _apply_filters(value, metadata["filters"])
    return value
