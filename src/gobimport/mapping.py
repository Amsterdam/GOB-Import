"""Mapping

Reads a mapping from a file

"""
import json

from gobcore.exceptions import GOBException


def in_datadir(filename: str):
    return f'data/{filename}'


DATASET_FILES = {
    "brk:kadastraleobjecten": in_datadir("brk.kadastraleobjecten.json"),
    "brk:zakelijkerechten": in_datadir("brk.zakelijkerechten.json"),
}


def get_dataset_file_location(dataset_id: str):
    try:
        return DATASET_FILES[dataset_id]
    except KeyError:
        raise GOBException(f"Dataset file for dataset {dataset_id} not found")


def get_mapping(input_name):
    """
    Read a mapping from a file

    :param input_name: name of the file that contains the mapping
    :return: an object that contains the mapping
    """
    with open(input_name) as file:
        return json.load(file)
