CODE_TABLE_FIELDS = ['code', 'omschrijving']


def _enrich_nummeraanduidingen(entities):
    """Enrich nummeraanduidingen

    :param entities: a list of entities imported from the source
    :return:
    """
    for entity in entities:
        _extract_dossier(entity)


def _enrich_verblijfsobjecten(entities):
    """Enrich verblijfsobjecten

    :param entities: a list of entities imported from the source
    :return:
    """
    for entity in entities:
        _extract_dossier(entity)

        gebruiksdoelen = entity['gebruiksdoel'].split(";")
        entity['gebruiksdoel'] = []
        for gebruiksdoel in gebruiksdoelen:
            entity['gebruiksdoel'].append(_extract_code_table(gebruiksdoel, CODE_TABLE_FIELDS))

        # Extract code tables for fields
        _extract_code_tables(entity, ['gebruiksdoel_woonfunctie', 'gebruiksdoel_gezondheidszorg'])

        # Toegang can be a multivalue code table
        if entity['toegang']:
            toegangen = entity['toegang'].split(";")
            entity['toegang'] = []
            for toegang in toegangen:
                entity['toegang'].append(_extract_code_table(toegang, CODE_TABLE_FIELDS))

        if entity['pandidentificatie']:
            entity['pandidentificatie'] = entity['pandidentificatie'].split(";")


def _enrich_panden(entities):
    """Enrich panden

    :param entities: a list of entities imported from the source
    :return:
    """
    for entity in entities:
        _extract_dossier(entity)


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
