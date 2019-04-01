"""
BAG enrichment

"""
from gobimport.enricher.enricher import Enricher

CODE_TABLE_FIELDS = ['code', 'omschrijving']


class BAGEnricher(Enricher):

    @classmethod
    def enriches(cls, catalog_name, entity_name):
        if catalog_name == "bag":
            enricher = BAGEnricher(catalog_name, entity_name)
            return enricher._enrich_entity is not None

    def __init__(self, _, entity_name):
        super().__init__({
            "woonplaatsen": self.enrich_woonplaats,
            "openbareruimtes": self.enrich_openbareruimte,
            "nummeraanduidingen": self.enrich_nummeraanduiding,
            "ligplaatsen": self.enrich_ligplaats,
            "standplaatsen": self.enrich_standplaats,
            "verblijfsobjecten": self.enrich_verblijfsobject,
            "panden": self.enrich_pand,
        }, entity_name)

    def enrich_woonplaats(self, woonplaats):
        _extract_dossier(woonplaats)

    def enrich_openbareruimte(self, openbareruimte):
        _extract_dossier(openbareruimte)

    def enrich_nummeraanduiding(self, nummeraanduiding):
        _extract_dossier(nummeraanduiding)

    def enrich_ligplaats(self, ligplaats):
        _extract_dossier(ligplaats)

    def enrich_standplaats(self, standplaats):
        _extract_dossier(standplaats)

    def enrich_pand(self, pand):
        _extract_dossier(pand)

    def enrich_verblijfsobject(self, verblijfsobject):
        _extract_dossier(verblijfsobject)

        gebruiksdoelen = verblijfsobject['gebruiksdoel'].split(";")
        verblijfsobject['gebruiksdoel'] = []
        for gebruiksdoel in gebruiksdoelen:
            verblijfsobject['gebruiksdoel'].append(_extract_code_table(gebruiksdoel, CODE_TABLE_FIELDS))

        # Extract code tables for fields
        _extract_code_tables(verblijfsobject, ['gebruiksdoel_woonfunctie', 'gebruiksdoel_gezondheidszorg'])

        # Toegang can be a multivalue code table
        if verblijfsobject['toegang']:
            toegangen = verblijfsobject['toegang'].split(";")
            verblijfsobject['toegang'] = []
            for toegang in toegangen:
                verblijfsobject['toegang'].append(_extract_code_table(toegang, CODE_TABLE_FIELDS))

        if verblijfsobject['pandidentificatie']:
            verblijfsobject['pandidentificatie'] = verblijfsobject['pandidentificatie'].split(";")


def _extract_dossier(entity):
    """Extract dossier into an array in the entity

    :param entity: an imported entity
    :return:
    """
    entity['dossier'] = entity['dossier'].split(";")


def _extract_code_tables(entity, fields):
    """Extract code tables for a list of field

    Code tables are build using CODE_TABLE_FIELDS and updated on the entity

    :param entity: an imported entity
    :return:
    """
    for field in fields:
        if entity[field]:
            entity[field] = _extract_code_table(entity[field], CODE_TABLE_FIELDS)


def _extract_code_table(value, fields, separator="|"):
    """Extract code table for a list of fields

    Splits a value on the separator and returns the values mapped on a list of fields

    :param value: a value to extract into a code table
    :param fields: a list of field to map onto
    :param separator: the value to separate the string on
    :return:
    """
    code_table = {}
    values = value.split(separator)
    for count, value in enumerate(values):
        code_table[fields[count]] = value
    return code_table
