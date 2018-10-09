"""Data converion logic

This module contains the logic to convert input data to GOB typed data

Todo:
    The current logic is bound to CSV files (especially the pandas.isnull logic to test for null valus)

"""
import re

from gobcore.typesystem import get_gob_type
from gobcore.model import GOBModel
from gobcore.exceptions import GOBException

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


def _extract_field(row, metadata, typeinfo):
    """
    Extract a field from a row given the corresponding metadata

    :param row: the data row
    :param metadata: the mapping definition
    :param typeinfo: the GOB model info
    :return: the string value of a field specified by the field's metadata, based on the values in row
    """
    field_type = typeinfo['type']
    field_source = metadata['source_mapping']

    gob_type = get_gob_type(field_type)

    kwargs = {k: v for k, v in metadata.items() if k not in ['type', 'source_mapping', 'filters']}

    value = row[field_source]
    if "filters" in metadata:
        # Apply any filters to the raw value
        value = _apply_filters(value, metadata["filters"])

    return gob_type.from_value(value, **kwargs)


def convert_data(data, dataset):
    """
    Convert the given data using the definitions in the dataset

    :param data:
    :param dataset:
    :return: An array of data items (entities)
    """
    entities = []

    gob_model = GOBModel()
    entity_model = gob_model.get_model(dataset["entity"])["fields"]

    mapping = dataset['gob_mapping']

    # Extract the fields that have a source mapping defined
    extract_fields = [field for field, meta in mapping.items() if 'source_mapping' in meta]

    for row in data:
        # extract source fields into target entity
        target_entity = {field: _extract_field(row, mapping[field], entity_model[field]) for field in extract_fields}

        # add explicit source id, as string, to target_entity
        source_id_field = dataset['source']['entity_id']
        source_id_value = row[source_id_field]
        source_id_str_value = str(get_gob_type("GOB.String").from_value(source_id_value))

        target_entity['_source_id'] = source_id_str_value

        entities.append(target_entity)

    return entities
