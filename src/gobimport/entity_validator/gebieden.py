""" Gebieden specific validation

Validations which need to happen after converting the data to GOBModel.
"""

import datetime

from gobcore.logging.logger import logger
from gobcore.model import FIELD
from gobcore.quality.issue import QA_CHECK, QA_LEVEL, Issue, log_issue


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
        if entity[FIELD.START_VALIDITY] > datetime.datetime.utcnow():
            log_issue(logger, QA_LEVEL.WARNING,
                      Issue(QA_CHECK.Value_not_in_future, entity, self.source_id, FIELD.START_VALIDITY))

    def validate_buurt(self, entity):
        """
        Validate buurten

        Checks that are being performed:

        - documentdatum can not be after eind_geldigheid (warning)

        :param entities: the list of entities
        :return:
        """
        # get eind_geldigheid or use current date
        eind_geldigheid = entity['eind_geldigheid'] if entity['eind_geldigheid'] else datetime.datetime.utcnow()

        datum = 'documentdatum'  # typ: date
        if entity[datum] and entity[datum] > eind_geldigheid.date():
            # documentdatum should not be after eind_geldigheid
            self.date_comparison_issue(entity, datum, 'eind_geldigheid')

        datum = 'registratiedatum'  # typ: datetime
        if entity[datum] and entity[datum] > eind_geldigheid:
            # registratiedatum should not be after eind_geldigheid
            self.date_comparison_issue(entity, datum, 'eind_geldigheid')

    def date_comparison_issue(self, entity, date_field, compare_date_field):
        """
        Log date comparison

        Logs the a warning for a date comparison between 2 fields

        :param entity: the entity which is compared
        :param date_field: field name of the date
        :param compare_date_field: field name of the compared date
        :return:
        """
        log_issue(logger, QA_LEVEL.WARNING,
                  Issue(QA_CHECK.Value_not_after, entity, self.source_id, date_field, compared_to=compare_date_field))
