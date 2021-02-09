""" Gebieden specific validation

Validations which need to happen after converting the data to GOBModel.
"""

from collections import defaultdict
from functools import reduce

from gobcore.logging.logger import logger
from gobcore.quality.issue import Issue, QA_CHECK, QA_LEVEL, log_issue

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
            "verblijfsobjecten": self.validate_verblijfsobject,
            "standplaatsen": self.validate_standplaats,
            "ligplaatsen": self.validate_ligplaats,
        }.get(entity_name)
        self.source_id = source_id
        self.validated = True

    def result(self):
        return self.validated

    def validate(self, entity):
        # Only validate objects from Amsterdam (0363) since Weesp does not have the plus-gegevens
        if entity.get('identificatie', '').startswith('0363'):
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
                            compared_to="hoogste_bouwlaag and laagste_bouwlaag combined",
                            compared_to_value=counted_bouwlagen))

        if not aantal_bouwlagen and all([value is not None for value in [laagste_bouwlaag, hoogste_bouwlaag]]):
            log_issue(logger, QA_LEVEL.WARNING,
                      Issue(QA_CHECK.Value_aantal_bouwlagen_not_filled, entity, self.source_id, "aantal_bouwlagen"))

    def validate_verblijfsobject(self, entity):
        """
        Validate verblijfsobjecten

        Checks that are being performed:

        Value_gebruiksdoel_gezondheidszorgfunctie_should_match = {
        - gebruiksdoel should be in the domain of possible options
        - gebruiksdoel_woonfunctie can only be filled if one of gebruiksdoel is woonfunctie
        - gebruiksdoel_gezondheidszorgfunctie can only be filled if one of gebruiksdoel is gezondheidszorgfunctie
        - aantal_eenheden_complex can only be filled if 'complex' is in one of woonfunctie or gezondheidszorgfunctie

        :param entities: the list of entities
        :return:
        """
        # Get all the gebruiksdoelen
        gebruiksdoelen = [gebruiksdoel.get('omschrijving') for gebruiksdoel in entity.get('gebruiksdoel', [{}])]

        self._check_gebruiksdoelen_exist(entity, gebruiksdoelen)
        self._check_gebruiksdoel_plus(entity, gebruiksdoelen)
        self._check_aantal_eenheden_complex(entity)

    def validate_standplaats(self, entity):
        gebruiksdoelen = [gebruiksdoel.get('omschrijving', '').lower() for gebruiksdoel in
                          entity.get('gebruiksdoel', [])]
        self._check_gebruiksdoelen_exist(entity, gebruiksdoelen)
        self._check_gebruiksdoelen_duplicates(entity, gebruiksdoelen)

    def validate_ligplaats(self, entity):
        gebruiksdoelen = [gebruiksdoel.get('omschrijving', '').lower() for gebruiksdoel in
                          entity.get('gebruiksdoel', [])]
        self._check_gebruiksdoelen_exist(entity, gebruiksdoelen)
        self._check_gebruiksdoelen_duplicates(entity, gebruiksdoelen)

    def _check_gebruiksdoelen_exist(self, entity: dict, gebruiksdoelen: list[str]):
        for gebruiksdoel in gebruiksdoelen:
            if gebruiksdoel not in VALID_GEBRUIKSDOEL_DOMAIN:
                log_issue(logger, QA_LEVEL.WARNING,
                          Issue(QA_CHECK.Value_gebruiksdoel_in_domain, entity, self.source_id, 'gebruiksdoel'))
                # Stop checking if the issue has occured, the whole list will be in the data warning
                break

    def _check_gebruiksdoelen_duplicates(self, entity: dict, gebruiksdoelen: list[str]):
        counts = reduce(lambda d, x: d | {x: d[x] + 1}, gebruiksdoelen, defaultdict(int))
        if [v for v in counts.values() if v > 1]:
            log_issue(
                logger,
                QA_LEVEL.WARNING,
                Issue(QA_CHECK.Value_duplicates, entity, self.source_id, 'gebruiksdoel')
            )

    def _check_gebruiksdoel_plus(self, entity, gebruiksdoelen):
        """
        The value of the gebruiksdoel_plus (woonfunctie or gezondheidszorgfunctie) may only be filled if
        gebruiksdoel is either woonfunctie or gezondheidszorgfunctie.
        """
        qa_checks = {
            'woonfunctie': QA_CHECK.Value_gebruiksdoel_woonfunctie_should_match,
            'gezondheidszorgfunctie': QA_CHECK.Value_gebruiksdoel_gezondheidszorgfunctie_should_match
        }

        # Check both woonfunctie and gezondheidszorgfunctie
        for check_value in ['woonfunctie', 'gezondheidszorgfunctie']:
            attribute_name = f'gebruiksdoel_{check_value}'
            attribute_value = entity.get(attribute_name, {}).get('omschrijving')

            if attribute_value and check_value not in gebruiksdoelen:
                log_issue(logger, QA_LEVEL.WARNING,
                          Issue(qa_checks[check_value], entity, self.source_id,
                                attribute_name, compared_to='gebruiksdoel'))

    def _check_aantal_eenheden_complex(self, entity):
        aantal_eenheden_complex = entity.get('aantal_eenheden_complex')

        check_attributes = ['gebruiksdoel_woonfunctie', 'gebruiksdoel_gezondheidszorgfunctie']
        check_values = [entity.get(attr, {}).get('omschrijving', '') or '' for attr in check_attributes]

        # If aantal_eenheden_complex is filled and complex not in the check values log a data warning
        if aantal_eenheden_complex is not None and all('complex' not in value.lower() for value in check_values):
            log_issue(logger, QA_LEVEL.WARNING,
                      Issue(QA_CHECK.Value_aantal_eenheden_complex_should_be_empty, entity, self.source_id,
                            'aantal_eenheden_complex',
                            compared_to='gebruiksdoel_woonfunctie and gebruiksdoel_gezondheidszorgfunctie',
                            compared_to_value=', '.join(check_values)))

        # If complex in one of the check values, but aantal_eenheden_complex is not filled, log a data warning
        if any('complex' in value.lower() for value in check_values) and not aantal_eenheden_complex:
            log_issue(logger, QA_LEVEL.WARNING,
                      Issue(QA_CHECK.Value_aantal_eenheden_complex_should_be_filled, entity, self.source_id,
                            'aantal_eenheden_complex',
                            compared_to='gebruiksdoel_woonfunctie and gebruiksdoel_gezondheidszorgfunctie',
                            compared_to_value=', '.join(check_values)))
