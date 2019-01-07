""" Validator

Basic validation logic

The first version implements only the most basic validation logic
In order to prepare a more generic validation approach the validation has been set up by means of regular expressions.

"""
from gobimport.enricher.meetbouten import _enrich_metingen
from gobimport.enricher.gebieden import _enrich_buurten, _enrich_wijken


def enrich(catalogue, entity_name, entities, log):
    """
    Enrich each entity in the list of entities for a given entity name

    :param catalogue: the name of the catalogue, e.g. meetbouten
    :param entity_name: the name of the entity, e.g. meetbouten
    :param entities: the list of entities
    :param log: log function of the import client
    :return:
    """
    enrichers = {
        "metingen": _enrich_metingen,
        "buurten": _enrich_buurten,
        "wijken": _enrich_wijken,
    }

    try:
        enrich_entities = enrichers[entity_name]
    except KeyError:
        return

    enrich_entities(entities, log)
