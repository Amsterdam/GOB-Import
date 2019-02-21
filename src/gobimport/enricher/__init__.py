""" Enricher

This enricher calls some specific functions for collections to add missing values in the source.
"""
from gobimport.enricher.meetbouten import _enrich_metingen
from gobimport.enricher.gebieden import _enrich_buurten, _enrich_wijken, enrich_ggwgebieden, enrich_ggpgebieden
from gobimport.enricher.bag import _enrich_nummeraanduidingen, _enrich_verblijfsobjecten, _enrich_panden


def enrich(catalogue, entity_name, entities):
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
        "ggwgebieden": enrich_ggwgebieden,
        "ggpgebieden": enrich_ggpgebieden,
        "nummeraanduidingen": _enrich_nummeraanduidingen,
        "verblijfsobjecten": _enrich_verblijfsobjecten,
        "panden": _enrich_panden,
    }

    try:
        enrich_entities = enrichers[entity_name]
    except KeyError:
        return

    enrich_entities(entities)
