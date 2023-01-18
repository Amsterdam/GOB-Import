"""Data conversion logic.

This module contains the logic to convert input data to GOB typed data.

Todo:
    The current logic is bound to CSV files (especially the pandas.isnull logic to test for null values)
"""

import re
from decimal import Decimal
from typing import Any, Union, cast

from gobcore.exceptions import GOBException, GOBTypeException
from gobcore.logging.logger import logger
from gobcore.model.metadata import FIELD
from gobcore.typesystem import get_gob_type_from_info, get_value
from gobimport import gob_model


class Converter:
    """Convert data to GOB entity."""

    def __init__(self, catalog_name, entity_name, input_spec):
        self.gob_model = gob_model
        collection = self.gob_model[catalog_name]["collections"][entity_name]

        self.input_spec = input_spec
        self.mapping = input_spec["gob_mapping"]
        self.fields = collection["all_fields"]

        # Get the fieldnames for the id and seqnr fields
        self.entity_id = input_spec["source"]["entity_id"]
        self.seqnr = self.mapping.get(FIELD.SEQNR, {}).get("source_mapping")

        # Extract the fields that have a source mapping defined
        self.extract_fields = [field for field, meta in self.mapping.items() if "source_mapping" in meta]

    def convert(self, row):
        """Convert the given data using the definitions in the dataset.

        :param row: data in external format
        :return: entity in GOB format
        """
        # Extract source fields into entity
        entity = {
            field: _extract_field(row, field, self.mapping[field], self.fields[field], self.entity_id, self.seqnr)
            for field in self.extract_fields
        }

        # Convert GOBTypes to Python objects
        entity = get_value(entity)

        # Add explicit source id, as string, to entity
        entity["_source_id"] = self.gob_model.get_source_id(entity=row, input_spec=self.input_spec)

        return entity


class MappinglessConverterAdapter:
    """Adapter for the Converter.

    Generates an input specification (mapping) where input row attributes are expected
    to have the same names as the model attributes.
    """

    def __init__(self, catalogue_name: str, entity_name: str, entity_id_attr: str):
        """Initialize Adapter for the Mappingless Converter.

        :param catalogue_name:
        :param entity_name:
        :param entity_id_attr: The name of the attribute that serves as the entity_id
        """
        self.collection = gob_model[catalogue_name]["collections"][entity_name]
        self.fields = self.collection["fields"]

        mapping = {field: {"source_mapping": field} for field in self.fields.keys()}

        input_spec = {
            "gob_mapping": mapping,
            "catalogue": catalogue_name,
            "entity": entity_name,
            "source": {"entity_id": entity_id_attr},
        }

        self.converter = Converter(catalogue_name, entity_name, input_spec)

    def convert(self, row: dict[str, Any]):
        """Convert the given data using the definitions in the dataset.

        :param row: data in external format
        :return: entity in GOB format
        """
        return self.converter.convert(row)


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
    """Tells if a field contains a literal value.

    Literal values start with '='

    :param field: field name
    :return: True if field is a literal value
    """
    return field[0] == "="


def _is_object_reference(field: str):
    """Check whether field references an object attribute.

    Used when the input value is a JSON column with a list of objects and we are
    referencing an attribute of these objects.

    Example: A JSON column with name 'json_column', with value
    [{'attribute': 'referenced_value'}, {'attribute': 'referenced_value2'}].
    The object reference in this case would be 'json_column.attribute'.

    :param field:
    :return:
    """
    return isinstance(field, str) and re.search(r"^[a-z_]+\.[a-z_]+$", field, flags=re.I) is not None


def _split_object_reference(field: str):
    """Split the object reference in the source column and attribute name.

    :param field:
    :return:
    """
    try:
        source, attr = field.split(".")
        return source, attr
    except ValueError as exc:
        raise GOBException("Object reference should contain exactly one dot (.)") from exc


def _literal_value(field):
    """Return the literal value of a literal field.

    Example: '=geometrie' returns 'geometrie'

    :param field: field name
    :return: the literal value of the field
    """
    return field[1:]


def _get_value(row, field):
    """Get the value for a field in a row.

    :param row: row of fields
    :param field: field name
    :return: the value of the specified field in the specified row
    """
    if _is_literal(field):
        # Literal value
        return _literal_value(field)
    if _is_object_reference(field):
        column, _ = _split_object_reference(field)
        return row.get(column)
    # Source value
    return row.get(field)


def _json_safe_value(value):
    """Transform value to a type that is safe for JSON serialisation.

    :param value:
    :return:
    """
    if isinstance(value, Decimal):
        return str(value)
    return value


def _extract_references(  # noqa: C901
    row: list[str], field_source: dict[Any, Any], field_type: str, force_list: bool = False
) -> Union[dict[Any, Any], list[dict[Any, Any]]]:
    """Create the dictionary as defined in field_source.

    Returns a list of dictionaries if field_type is GOB.ManyReference or force_list is True

    :param row:
    :param field_source: The source_mapping
    :param field_type: The field_type as string (GOB.xxxx)
    :param force_list: Force return of list of dicts, even if not a GOB.ManyReference
    :return:
    """
    FORMAT = "format"  # Optional parameter for ManyReference single string values
    if field_type == "GOB.ManyReference" or force_list:
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
                format = field_source.get(FORMAT, {}).get("split")
                source_values = source_value.split(format) if format else [source_value]
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
        value = cast(dict[Any, Any], {})  # type: ignore

        for attribute, source_mapping in field_source.items():
            source_value = _json_safe_value(_get_value(row, source_mapping))

            if _is_object_reference(source_mapping) and source_value:
                if not value:
                    value = {**source_value}  # type: ignore

                column, attr = _split_object_reference(source_mapping)

                if not isinstance(source_value, dict):
                    raise GOBException("References should be dicts when referencing by attribute")

                value[attribute] = source_value[attr]
            else:
                value[attribute] = source_value

    return value


def _clean_references(value):
    """Clean the references to return a dict with bronwaarde and broninfo.

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
    """Extract broninfo from the references.

    :param value: The reference
    :return: the cleaned reference
    """
    root_values = [FIELD.SOURCE_VALUE, FIELD.START_VALIDITY]

    # Move all attributes to source info if it's not one of the root_values
    source_value = {field: value[field] for field in root_values if field in value}
    source_info = {k: v for k, v in value.items() if k not in root_values}

    # Only add broninfo if there are additional fields
    if len(source_info.keys()) == 0:
        return {**source_value}
    return {**source_value, FIELD.SOURCE_INFO: source_info}


def _extract_field(row, field, metadata, typeinfo, entity_id_field=None, seqnr_field=None):
    """Extract a field from a row given the corresponding metadata.

    :param row: the data row
    :param metadata: the mapping definition
    :param typeinfo: the GOB model info
    :return: the string value of a field specified by the field's metadata, based on the values in row
    """
    field_type = typeinfo["type"]
    field_source = metadata["source_mapping"]

    gob_type = get_gob_type_from_info(typeinfo)

    kwargs = {k: v for k, v in metadata.items() if k not in ["type", "source_mapping", "filters"]}

    if isinstance(field_source, dict):
        value = _extract_references(row, field_source, field_type, metadata.get("force_list", False))
    else:
        value = _get_value(row, field_source)

    # Clean all references
    if field_type in ("GOB.Reference", "GOB.ManyReference") and value is not None:
        value = _clean_references(value)

    value = _apply_field_filters(metadata, value)

    try:
        return gob_type.from_value_secure(value, typeinfo, **kwargs)
    except GOBTypeException:
        # The row is still in the source format.
        report_row = _goblike_row(row, entity_id_field, seqnr_field)
        report_row[field] = value

        id = f"{report_row[FIELD.ID]}" + (f".{report_row[FIELD.SEQNR]}" if seqnr_field else "")
        logger.error(f"Error importing object with id {id}. Can't extract value for field {field}")
        return gob_type.from_value_secure(None, typeinfo, **kwargs)


def _goblike_row(row, entity_id_field, seqnr_field=None):
    """Convert the raw source row into a GOB-like row.

    Construct a "GOB-row" to report the issue.

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
        if isinstance(metadata["filters"], dict):
            for attribute, filters in metadata["filters"].items():
                value[attribute] = _apply_filters(value[attribute], filters)
        else:
            # Apply any filters to the raw value
            value = _apply_filters(value, metadata["filters"])
    return value
