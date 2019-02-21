""" Gebieden specific validation

Validations which need to happen after converting the data to GOBModel.
"""

import datetime

from gobcore.logging.logger import logger


def _validate_bouwblokken(entities, source_id):
    """
    Validate bouwblokken

    Checks that are being performed:

    - begin_geldigheid can not be in the future (fatal)

    :param entities: the list of entities
    :return:
    """
    validated = True

    for entity in entities:
        # begin_geldigheid can not be in the future
        # to_db is used to work around the typesystem: https://github.com/Amsterdam/GOB-Core/issues/127
        identificatie = str(entity[source_id])
        if entity['begin_geldigheid'].to_db > datetime.datetime.utcnow():
            msg = "begin_geldigheid can not be in the future"
            extra_data = {
                'id': msg,
                'data': {
                    'identificatie': identificatie,
                    'begin_geldigheid': entity['begin_geldigheid'],
                }
            }
            logger.error(msg, extra_data)
            validated = False

    return validated


def _validate_buurten(entities, source_id):
    """
    Validate buurten

    Checks that are being performed:

    - documentdatum can not be after eind_geldigheid (warning)

    :param entities: the list of entities
    :return:
    """
    validated = True

    for entity in entities:
        # get eind_geldigheid or use current date
        # to_db is used to work around the typesystem: https://github.com/Amsterdam/GOB-Core/issues/127
        identificatie = str(entity[source_id])
        eind_geldigheid = entity['eind_geldigheid'].to_db if entity['eind_geldigheid'].to_db \
            else datetime.datetime.utcnow()
        documentdatum = entity['documentdatum'].to_db
        # documentdatum should not be after eind_geldigheid
        if documentdatum and documentdatum > eind_geldigheid:

            msg = "documentdatum can not be after eind_geldigheid"
            extra_data = {
                'id': msg,
                'data': {
                    'identificatie': identificatie,
                    'documentdatum': entity['documentdatum'],
                    'eind_geldigheid': entity['eind_geldigheid'],
                }
            }
            logger.warning(msg, extra_data)
    return validated
