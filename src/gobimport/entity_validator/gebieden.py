""" Gebieden specific validation

Validations which need to happen after converting the data to GOBModel.
"""

import datetime

from gobcore.logging.logger import logger


class GebiedenValidator:

    @classmethod
    def validates(cls, catalog_name, entity_name):
        """
        Tells wether this class validates the given catalog entity

        :param catalog_name:
        :param entity_name:
        :return:
        """
        if catalog_name == "gebieden":
            validator = GebiedenValidator(catalog_name, entity_name)
            return validator.validate_entity is not None

    def __init__(self, catalog_name, entity_name, source_id=None):
        self.validate_entity = {
            "buurten": self.validate_buurt,
            "bouwblokken": self.validate_bouwblok
        }.get(entity_name)
        self.source_id = source_id
        self.validated = True

    def result(self):
        return self.validated

    def validate(self, entity):
        self.validate_entity(entity)

    def validate_bouwblok(self, entity):
        """
        Validate bouwblok

        Checks that are being performed:

        - begin_geldigheid can not be in the future (not fatal)

        :param entities: the list of entities
        :return:
        """
        # begin_geldigheid can not be in the future
        identificatie = str(entity[self.source_id])
        if entity['begin_geldigheid'] > datetime.datetime.utcnow().date():
            msg = "begin_geldigheid can not be in the future"
            extra_data = {
                'id': msg,
                'data': {
                    'identificatie': identificatie,
                    'begin_geldigheid': entity['begin_geldigheid'].isoformat(),
                }
            }
            logger.warning(msg, extra_data)

    def validate_buurt(self, entity):
        """
        Validate buurten

        Checks that are being performed:

        - documentdatum can not be after eind_geldigheid (warning)

        :param entities: the list of entities
        :return:
        """
        # get eind_geldigheid or use current date
        eind_geldigheid = entity['eind_geldigheid'] if entity['eind_geldigheid'] \
            else datetime.datetime.utcnow().date()

        documentdatum = entity['documentdatum']
        # documentdatum should not be after eind_geldigheid
        if documentdatum and documentdatum > eind_geldigheid:
            self.log_date_comparison(entity, 'documentdatum', 'eind_geldigheid')

        registratiedatum = entity['registratiedatum']
        # registratiedatum should not be after eind_geldigheid
        if registratiedatum and registratiedatum.date() > eind_geldigheid:
            self.log_date_comparison(entity, 'registratiedatum', 'eind_geldigheid')

    def log_date_comparison(self, entity, date_field, compare_date_field):
        """
        Log date comparison

        Logs the a warning for a date comparison between 2 fields

        :param entity: the entity which is compared
        :param date_field: field name of the date
        :param compare_date_field: field name of the compared date
        :return:
        """
        identificatie = str(entity[self.source_id])
        msg = f"{date_field} can not be after {compare_date_field}"
        extra_data = {
            'id': msg,
            'data': {
                'identificatie': identificatie,
                '{date_field}': entity[date_field].isoformat(),
                '{compare_date_field}': entity[compare_date_field].isoformat(),
            }
        }
        logger.warning(msg, extra_data)
