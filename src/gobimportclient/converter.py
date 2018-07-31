import datetime
from pandas import pandas


def convert(config, raw_data):
    pass


def as_decimal(value):
    return str(value).strip().replace(",", ".") if not pandas.isnull(value) else None


def as_str(value):
    return str(value) if not pandas.isnull(value) else None


def as_boolean(value):
    return str(False) if value == 'N' else str(True)


def as_date(value, format):
    return datetime.datetime.strptime(str(value), format).date().strftime("%Y-%m-%d") \
        if not pandas.isnull(value) else None


def converted_value(value, type):
    if type == "string":
        return as_str(value)
    elif type == "decimal":
        return as_decimal(value)
    elif type == "boolean":
        return as_boolean(value)
    elif type == "date":
        return as_date(value, "%Y%m%d")
    else:
        raise NotImplementedError


def extract_from_source(row, metadata):
    field = metadata['source_mapping']
    value = row[field]
    return converted_value(value, metadata['type'])


def calculate_field(entity, metadata):
    if metadata['type'] == "geo_point":
        x_source = metadata['calculated'][0]
        y_source = metadata['calculated'][1]
        return f"POINT({entity[x_source]}, {entity[y_source]})",
    else:
        raise NotImplementedError


def convert_from_file(data, dataset):
    entities = []

    if dataset['source']['config']['filetype'] == 'CSV':
        model = dataset['gob_model']

        extract_fields = [field for field, meta in model.items() if 'source_mapping' in meta]
        calculate_fields = [field for field, desc in model.items() if 'calculated' in desc]

        for index, row in data.iterrows():
            target_entity = {field: extract_from_source(row, model[field]) for field in extract_fields}

            for field in calculate_fields:
                target_entity[field] = calculate_field(target_entity, model[field])

            entities.append(target_entity)
    else:
        raise NotImplementedError

    return entities
