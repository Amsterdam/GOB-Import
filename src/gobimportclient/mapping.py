"""Mapping

Reads a mapping from a file

"""
import json


def get_mapping(input_name):
    """
    Read a mapping from a file

    :param input_name: name of the file that contains the mapping
    :return: an object that contains the mapping
    """
    with open(input_name) as file:
        return json.load(file)
