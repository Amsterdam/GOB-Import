"""Data converion logic

This module contains the logic to convert input data to GOB typed data

Todo:
    The current logic is bound to CSV files (especially the pandas.isnull logic to test for null valus)

"""
from gobcore.typesystem import get_gob_type
from gobcore.model import GOBModel


def _extract_field(row, metadata, typeinfo):
    """
    Extract a field from a row given the corresponding metadata

    Examples:
        given the metadata: {
            "type": "GOB.String",
            "source_mapping": "MEBO_ID"
        }
        The field "MEBO_ID" of the specified row is read as a "GOB.String" value using
        the mapper method as specified in the type_mapper variable.

        given the metadata: {
          "type": "GOB.Geo.Point",
          "srid": "RD",
          "source_mapping": {
            'x': "BOUT_XCOORD",
            'y': "BOUT_YCOORD"
          }
        }
        the values of srid, and sourcemapping are used to construct a (composite) field
        of type "GOB.Geo.Point".

    :param row:
    :param metadata:
    :return: the string value of a field specified by the field's metadata, based on the values in row
    """
    field_type = typeinfo['type']
    field_source = metadata['source_mapping']

    gob_type = get_gob_type(field_type)

    kwargs = {k: v for k, v in metadata.items() if k not in ['type', 'source_mapping']}
    value = row[field_source]
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
    entity_model = gob_model.get_model(dataset["entity"])["attributes"]

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
