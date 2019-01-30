""" Gebieden specific validation

Validations which need to happen after converting the data to GOBModel.
"""

import datetime


def _validate_bouwblokken(entities, log):
    """
    Validate bouwblokken

    Checks that are being performed:

    - begin_geldigheid can not be in the future (fatal)

    :param entities: the list of entities
    :param log: log function of the import client
    :return:
    """
    validated = True

    for entity in entities:
        # begin_geldigheid can not be in the future
        if entity['begin_geldigheid'].to_db > datetime.datetime.now():
            msg = "begin_geldigheid can not be in the future"
            extra_data = {
                'data': {
                    'identificatie': entity['identificatie'],
                    'begin_geldigheid': entity['begin_geldigheid'],
                }
            }
            log("error", msg, extra_data)
            validated = False

    return validated


def _validate_buurten(entities, log):
    """
    Validate buurten

    Checks that are being performed:

    - documentdatum can not be after eind_geldigheid (warning)

    :param entities: the list of entities
    :param log: log function of the import client
    :return:
    """
    validated = True

    for entity in entities:
        # get eind_geldigheid or use current date
        eind_geldigheid = entity['eind_geldigheid'].to_db if entity['eind_geldigheid'].to_db \
            else datetime.datetime.now()
        documentdatum = entity['documentdatum'].to_db
        # documentdatum should not be after eind_geldigheid
        if documentdatum and documentdatum > eind_geldigheid:

            msg = "documentdatum can not be after eind_geldigheid"
            extra_data = {
                'data': {
                    'identificatie': entity['identificatie'],
                    'documentdatum': entity['documentdatum'],
                    'eind_geldigheid': entity['eind_geldigheid'],
                }
            }
            log("warning", msg, extra_data)
    return validated
