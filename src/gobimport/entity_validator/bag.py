""" Gebieden specific validation

Validations which need to happen after converting the data to GOBModel.
"""

from gobcore.logging.logger import logger
from gobcore.quality.issue import QA_CHECK, QA_LEVEL, Issue, log_issue

VALID_GEBRUIKSDOEL_DOMAIN = [
    'woonfunctie',
    'bijeenkomstfunctie',
    'celfunctie',
    'gezondheidszorgfunctie',
    'industriefunctie',
    'kantoorfunctie',
    'logiesfunctie',
    'onderwijsfunctie',
    'sportfunctie',
    'winkelfunctie',
    'overige gebruiksfunctie',
]


class BAGValidator:

    @classmethod
    def validates(cls, catalog_name, entity_name):
        """
        Tells wether this class validates the given catalog entity

        :param catalog_name:
        :param entity_name:
        :return:
        """
        if catalog_name == "bag":
            validator = BAGValidator(catalog_name, entity_name)
            return validator.validate_entity is not None

    def __init__(self, catalog_name, entity_name, source_id=None):
        self.validate_entity = {
            "panden": self.validate_pand,
            "verblijfsobjecten": self.validate_verblijfsobject
        }.get(entity_name)
        self.source_id = source_id
        self.validated = True

    def result(self):
        return self.validated

    def validate(self, entity):
        self.validate_entity(entity)

    def validate_pand(self, entity):
        """
        Validate pand

        Checks that are being performed:

        - aantal_bouwlagen does not match the highest and lowest bouwlagen
        - aantal_bouwlagen isn't filled but hoogste and laagste bouwlaag is

        :param entities: the list of entities
        :return:
        """
        laagste_bouwlaag = entity.get('laagste_bouwlaag')
        hoogste_bouwlaag = entity.get('hoogste_bouwlaag')
        aantal_bouwlagen = entity.get('aantal_bouwlagen')

        counted_bouwlagen = None

        if all(b is not None for b in [laagste_bouwlaag, hoogste_bouwlaag]):
            count_ground_floor = 1 if laagste_bouwlaag < 1 else 0
            counted_bouwlagen = (hoogste_bouwlaag + count_ground_floor) - laagste_bouwlaag

        # aantal_bouwlagen should match the highest and lowest value
        if all([aantal_bouwlagen, counted_bouwlagen]) and aantal_bouwlagen != counted_bouwlagen:
            log_issue(logger, QA_LEVEL.WARNING,
                      Issue(QA_CHECK.Value_aantal_bouwlagen_should_match, entity, self.source_id, "aantal_bouwlagen",
                            compared_to="hoogste_bouwlaag and laagste_bouwlaag", compared_to_value=counted_bouwlagen))

        if not aantal_bouwlagen and all([laagste_bouwlaag, hoogste_bouwlaag]):
            log_issue(logger, QA_LEVEL.WARNING,
                      Issue(QA_CHECK.Value_aantal_bouwlagen_not_filled, entity, self.source_id, "aantal_bouwlagen"))

    def validate_verblijfsobject(self, entity):
        """
        Validate verblijfsobjecten

        Checks that are being performed:

        Value_gebruiksdoel_gezondheidszorgfunctie_should_match = {
        - gebruiksdoel should be in the domain of possible options
        - gebruiksdoel_woonfunctie can only be filled if gebruiksdoel is woonfunctie
        - gebruiksdoel_gezondheidszorgfunctie can only be filled if gebruiksdoel is gezondheidszorgfunctie
        - aantal_eenheden_complex can only be filled if 'complex' is in one of woonfunctie or gezondheidszorgfunctie

        :param entities: the list of entities
        :return:
        """
        # Get the first gebruiksdoel
        gebruiksdoel = entity.get('gebruiksdoel', [{}])[0].get('omschrijving')
        gebruiksdoel_woonfunctie = entity.get('gebruiksdoel_woonfunctie', {}).get('omschrijving')
        gebruiksdoel_gezondheidszorgfunctie = entity.get('gebruiksdoel_gezondheidszorgfunctie', {}).get('omschrijving')
        aantal_eenheden_complex = entity.get('aantal_eenheden_complex')

        if gebruiksdoel not in VALID_GEBRUIKSDOEL_DOMAIN:
            log_issue(logger, QA_LEVEL.WARNING,
                      Issue(QA_CHECK.Value_gebruiksdoel_in_domain, entity, self.source_id, 'gebruiksdoel'))

        if gebruiksdoel_woonfunctie and gebruiksdoel != 'woonfunctie':
            log_issue(logger, QA_LEVEL.WARNING,
                      Issue(QA_CHECK.Value_gebruiksdoel_woonfunctie_should_match, entity, self.source_id,
                            'gebruiksdoel_woonfunctie', compared_to='gebruiksdoel'))

        if gebruiksdoel_gezondheidszorgfunctie and gebruiksdoel != 'gezondheidszorgfunctie':
            log_issue(logger, QA_LEVEL.WARNING,
                      Issue(QA_CHECK.Value_gebruiksdoel_gezondheidszorgfunctie_should_match, entity, self.source_id,
                            'gebruiksdoel_gezondheidszorgfunctie', compared_to='gebruiksdoel'))

        # Check with either woonfunctie of gezondheidszorgfunctie when aantal_eenheden_complex is filled
        check_attr = 'gebruiksdoel_woonfunctie' if gebruiksdoel_woonfunctie \
                     else 'gebruiksdoel_gezondheidszorgfunctie'

        check_value = entity.get(check_attr, {}).get('omschrijving', '')
        check_value = '' if check_value is None else check_value

        if aantal_eenheden_complex is not None and 'complex' not in check_value.lower():
            log_issue(logger, QA_LEVEL.WARNING,
                      Issue(QA_CHECK.Value_aantal_eenheden_complex_filled, entity, self.source_id,
                            'aantal_eenheden_complex', compared_to=check_attr))

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
