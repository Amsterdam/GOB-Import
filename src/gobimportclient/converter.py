"""Data converion logic

This module contains the logic to convert input data to GOB typed data

Todo:
    The current logic is bound to CSV files (especially the pandas.isnull logic to test for null valus)

"""
import datetime
from pandas import pandas


def as_decimal(value):
    """
    Returns value as a string containing a decimal value
    :param value:
    :return: the string that holds the value in decimal format or None
    """
    return str(value).strip().replace(",", ".") if not pandas.isnull(value) else None


def as_number(value):
    """
    Returns value as a string containing an integer value
    :param value:
    :return: the string that holds the value in integer format or None
    """
    decimal = as_decimal(value)
    return str.split(decimal, ".")[0] if decimal is not None else None


def as_str(value):
    """
    Returns value as a string
    :param value:
    :return: the string that holds the value or None
    """
    return str(value) if not pandas.isnull(value) else None


def as_char(value):
    """
    Returns value as a string containing a single character value
    :param value:
    :return: the string that holds the value in single character format or None
    """
    return str(value)[0] if not pandas.isnull(value) else None


def as_boolean(value):
    """
    Returns value as a string containing a boolean value, default "True"

    Todo:
        Determine if the default value True is OK

    :param value:
    :return: the string that holds the boolean value
    """
    return str(False) if value == 'N' else str(True)


def as_date(value):
    """
    Returns value as a string containing a date value in ISO 8601 format
    :param value:
    :return: the string that holds the value in data or None
    """
    return datetime.datetime.strptime(str(value), "%Y%m%d").date().strftime("%Y-%m-%d") \
        if not pandas.isnull(value) else None


type_mapper = {
    "GOB.String": as_str,
    "GOB.Character": as_char,
    "GOB.Decimal": as_decimal,
    "GOB.Number": as_number,
    "GOB.Boolean": as_boolean,
    "GOB.Date": as_date
}


def extract_field(row, metadata):
    """
    Extract a field from a row given the corresponding metadata

    Example:
        given the metadata: {
            "type": "GOB.String",
            "source_mapping": "MEBO_ID"
        }
        The field "MEBO_ID" of the specified row is read as a "GOB.String" value using
        the mapper method as specified in the type_mapper variable.

    :param row:
    :param metadata:
    :return: the value of a field in a row specified by the field's metadata
    """
    field = metadata['source_mapping']
    type = metadata['type']
    value = row[field]
    if type in type_mapper:
        return type_mapper[type](value)
    raise NotImplementedError("Undefined type")


def calculate_field(entity, metadata):
    """
    Calculate the value of a field by using it's metadata

    Example:
        given the metadata: {
          "type": "GOB.Geo.Point",
          "srid": "RD",
          "calculated": [
            "xcoordinaat",
            "ycoordinaat"
          ]
        }
        the values of xcoordinaat and ycoordinaat in the entity are used to construct a (calculated) field
        of type "GOB.Geo.Point".

    Todo:
        The SR identifier should be used

    :param entity:
    :param metadata:
    :return: The calculate field value for en entity by using the field's metadata
    """
    if metadata['type'] == "GOB.Geo.Point":
        x_source_field = metadata['calculated'][0]
        y_source_field = metadata['calculated'][1]
        return f"POINT({entity[x_source_field]}, {entity[y_source_field]})"
    else:
        raise NotImplementedError


def convert_from_file(data, dataset):
    """
    Convert the given data using the definitions in the dataset

    :param data:
    :param dataset:
    :return: An array of data items (entities)
    """
    entities = []

    if dataset['source']['config']['filetype'] == 'CSV':
        model = dataset['gob_model']

        # Extract the fields that have a source mapping defined
        extract_fields = [field for field, meta in model.items() if 'source_mapping' in meta]

        # Extract the fields that are calculated on basis of the extracted fields (extract_fields)
        calculate_fields = [field for field, desc in model.items() if 'calculated' in desc]

        for index, row in data.iterrows():
            # extract source fields into target entity
            target_entity = {field: extract_field(row, model[field]) for field in extract_fields}

            # add explicit source id, as string, to target_entity
            source_id_field = dataset['source']['entity_id']
            target_entity['_source_id'] = type_mapper["GOB.String"](row[source_id_field])

            # Use the extracted fields in target_entity to derive the calculated fields
            for field in calculate_fields:
                target_entity[field] = calculate_field(target_entity, model[field])

            entities.append(target_entity)
    else:
        raise NotImplementedError

    return entities
