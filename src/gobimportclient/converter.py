import datetime
from pandas import pandas


def convert(config, raw_data):
    pass


def as_decimal(value):
    return str(value).strip().replace(",", ".") if not pandas.isnull(value) else None


def as_number(value):
    decimal = as_decimal(value)
    return str.split(decimal, ".")[0] if decimal is not None else None


def as_str(value):
    return str(value) if not pandas.isnull(value) else None


def as_char(value):
    return str(value)[0] if not pandas.isnull(value) else None


def as_boolean(value):
    return str(False) if value == 'N' else str(True)


def as_date(value):
    return datetime.datetime.strptime(str(value), "%Y%m%d").date().strftime("%Y-%m-%d") \
        if not pandas.isnull(value) else None


type_mapper = {
    "string": as_str,
    "char": as_char,
    "decimal": as_decimal,
    "number": as_number,
    "boolean": as_boolean,
    "date": as_date
}


def extract_field(row, metadata):
    field = metadata['source_mapping']
    type = metadata['type']
    value = row[field]
    if type in type_mapper:
        return type_mapper[type](value)
    raise NotImplementedError("Undefined type")


def calculate_field(entity, metadata):
    if metadata['type'] == "geo_point":
        x_source_field = metadata['calculated'][0]
        y_source_field = metadata['calculated'][1]
        return f"POINT({entity[x_source_field]}, {entity[y_source_field]})"
    else:
        raise NotImplementedError


def convert_from_file(data, dataset):
    entities = []

    if dataset['source']['config']['filetype'] == 'CSV':
        model = dataset['gob_model']

        extract_fields = [field for field, meta in model.items() if 'source_mapping' in meta]
        calculate_fields = [field for field, desc in model.items() if 'calculated' in desc]

        for index, row in data.iterrows():
            # extract source fields into target entity
            target_entity = {field: extract_field(row, model[field]) for field in extract_fields}

            # add explicit source id, as string, to target_entity
            source_id_field = dataset['source']['entity_id']
            target_entity['_source_id'] = type_mapper["string"](row[source_id_field])

            for field in calculate_fields:
                target_entity[field] = calculate_field(target_entity, model[field])

            entities.append(target_entity)
    else:
        raise NotImplementedError

    return entities
