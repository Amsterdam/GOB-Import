""" Validator

Basic validation logic

The first version implements only the most basic validation logic
In order to prepare a more generic validation approach the validation has been set up by means of regular expressions.

"""
import re


def _validate_meetbouten(entity):
    """
    Validate a single meetbout.

    Fails on any fatal validation check

    :param entity: a single meetbout
    :return: None
    """

    fatals = {
        "meetboutid": {
            "pattern": "^\d{8}$",
            "msg": "Meetboutid should consist of 8 numeric characters:"
        }
    }

    for attr, validation in fatals.items():
        pattern = validation["pattern"]
        value = str(entity[attr])
        msg = validation["msg"]
        assert re.compile(pattern).match(value), f"{msg}: '{value}'"


def validate(entity_name, entities):
    """
    Validate each entity in the list of entities for a given entity name

    :param entity_name: the name of the entity, e.g. meetbouten
    :param entities: the list of entities
    :return:
    """
    validators = {
        "meetbouten": _validate_meetbouten
    }

    validate_entity = validators[entity_name]

    for entity in entities:
        validate_entity(entity)
