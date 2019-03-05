"""Data converion logic

This module contains the logic to convert input data to GOB typed data

Todo:
    The current logic is bound to CSV files (especially the pandas.isnull logic to test for null valus)

"""
import re

from gobcore.typesystem import get_gob_type, get_value
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


def _extract_references(row, field_source, field_type):   # noqa: C901
    # If we can expect multiple references create an array of dicts
    if field_type == 'GOB.ManyReference':
        value = []
        # For each attribute in the source mapping, loop through all values and add them to the correct dict
        for attribute, source_mapping in field_source.items():
            if row[source_mapping]:
                for idx, v in enumerate(row[source_mapping]):
                    # Try to update the dictionary with the attribute and value or create a new dict
                    try:
                        value[idx].update({attribute: v})
                    except IndexError:
                        value.append({attribute: v})
    else:
        value = {attribute: row[source_mapping] for attribute, source_mapping in field_source.items()}

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

    if isinstance(field_source, dict):
        value = _extract_references(row, field_source, field_type)
    else:
        value = row[field_source]

    if "filters" in metadata:
        # If we are dealing with a dict, apply filters to the correct attribute
        if isinstance(metadata['filters'], dict):
            for attribute, filters in metadata['filters'].items():
                value[attribute] = _apply_filters(value[attribute], filters)
        else:
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
    collection = gob_model.get_collection(dataset['catalogue'], dataset['entity'])
    fields = collection['fields']

    mapping = dataset['gob_mapping']

    # Extract the fields that have a source mapping defined
    extract_fields = [field for field, meta in mapping.items() if 'source_mapping' in meta]

    n = 0
    while data:
        n += 1
        if n % 10000 == 0:
            print(n)

        # Delete original data
        row = data.pop(0)

        # extract source fields into entity
        entity = {field: _extract_field(row, mapping[field], fields[field]) for field in extract_fields}

        # Convert GOBTypes to python objects
        entity = get_value(entity)

        # add explicit source id, as string, to entity
        entity['_source_id'] = gob_model.get_source_id(entity=row, input_spec=dataset)

        # Save converted data
        entities.append(entity)

    return entities
