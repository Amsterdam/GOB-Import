"""Mapping

Reads a mapping from a file

"""
import json
import os

from collections import defaultdict

from gobcore.exceptions import GOBException

DATASET_DIR = 'data/'


def _build_dataset_locations_mapping():
    """Builds dataset locations mapping based on json files present in DATASET_DIR

    :return:
    """
    # Init 3-dimensional dict
    result = defaultdict(lambda: defaultdict(lambda: defaultdict(str)))

    for file in os.listdir(DATASET_DIR):
        filepath = DATASET_DIR + file
        if os.path.isfile(filepath) and file.endswith('.json'):
            try:
                mapping = get_mapping(filepath)
                catalogue = mapping['catalogue']
                collection = mapping['entity']
                application = mapping['source']['application']
            except (KeyError, json.decoder.JSONDecodeError):
                raise GOBException(f"Dataset file {filepath} invalid")
            result[catalogue][collection][application] = filepath
    return result


def get_dataset_file_location(catalogue: str, collection: str, application: str):
    try:
        return dataset_locations_mapping[catalogue][collection][application]
    except KeyError:
        raise GOBException(f"No dataset found for catalogue, collection, application combination: "
                           f"{catalogue}, {collection}, {application}")


def get_mapping(input_name):
    """
    Read a mapping from a file

    :param input_name: name of the file that contains the mapping
    :return: an object that contains the mapping
    """
    with open(input_name) as file:
        return json.load(file)


dataset_locations_mapping = _build_dataset_locations_mapping()
