"""Mapping

Reads a mapping from a file

"""
import json
import os

from collections import defaultdict

from gobcore.exceptions import GOBException

DATASET_DIR = os.path.join(os.path.dirname(__file__), '../data/')


def _build_dataset_locations_mapping():
    """Builds dataset locations mapping based on json files present in DATASET_DIR

    :return:
    """
    # Init 3-dimensional dict
    result = defaultdict(lambda: defaultdict(dict))

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


def get_dataset_file_location(catalogue: str, collection: str, application: str = None):
    """Returns the dataset file location for the given catalogue, collection, application combination. Application may
    be omitted when there is only one application available for given catalogue and collection.

    :param catalogue:
    :param collection:
    :param application:
    :return:
    """
    try:
        if not application:
            applications = dataset_locations_mapping[catalogue][collection]

            if len(applications.keys()) > 1:
                raise GOBException(f"Multiple applications found for catalogue, collection combination: "
                                   f"{catalogue}, {collection}. Please specify the application.")

            return next(iter(applications.values()))
        else:
            return dataset_locations_mapping[catalogue][collection][application]
    except (KeyError, IndexError, StopIteration):
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
