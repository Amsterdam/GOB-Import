"""
BAG enrichment

"""
from gobcore.logging.logger import logger

from gobimport.enricher.enricher import Enricher
from gobcore.quality.issue import QA_CHECK, QA_LEVEL, Issue, log_issue


CODE_TABLE_FIELDS = ['code', 'omschrijving']

FINANCIERINGSCODE_MAPPING = {
    '501': 'Eigen bouw (501)',
    '110': 'Sociale huursector woningwet (110)',
    '200': 'Premiehuur Profit (200)',
    '201': 'Premiehuur Profit voor 1985 (201)',
    '220': 'Wet Materiële Oorlogsschade (220)',
    '250': 'Huurwoningen Beleggers (250)',
    '271': 'Premiehuur Profit met gemeentegarantie (271)',
    '274': 'Sociale koop (voorheen Premiekoop A) (274)',
    '301': 'Premiekoop voor 1985 (301)',
    '373': 'Subsidie Premiekoop A (373)',
    '374': 'Subsidie Premiekoop B (374)',
    '375': 'Premiewoningen (375)',
    '466': 'V.S.E.B. (466)',
    '475': 'V.S.E.B. Premiekoop C (475)',
    '476': 'V.S.E.B. Premiehuur C (476)',
    '500': 'Ongesubsidieerde bouw (500)',
    '999': 'DEFAULT VOOR CONVERSIE',
    '477': 'Middeldure huur (477)',
    '478': 'Middeldure koop (478)',
}


class BAGEnricher(Enricher):

    @classmethod
    def enriches(cls, app_name, catalog_name, entity_name):
        if catalog_name == "bag":
            enricher = BAGEnricher(app_name, catalog_name, entity_name)
            return enricher._enrich_entity is not None

    def __init__(self, app_name, catalogue_name, entity_name):
        self.multiple_values_logged = False
        self.entity_name = entity_name

        super().__init__(app_name, catalogue_name, entity_name, methods={
            "nummeraanduidingen": self.enrich_nummeraanduiding,
            "verblijfsobjecten": self.enrich_verblijfsobject,
        })

    def enrich(self, entity):
        """
        Enrich a single entity, override with default enrichment for all BAG collections

        :param entity:
        :return:
        """

        if self._enrich_entity:
            self._enrich_entity(entity)

    def enrich_nummeraanduiding(self, nummeraanduiding):

        # ligt_in_woonplaats can have multiple values, use the last value and log a warning
        bronwaarde = nummeraanduiding['ligt_in_bag_woonplaats']
        if bronwaarde and ';' in bronwaarde:
            nummeraanduiding['ligt_in_bag_woonplaats'] = bronwaarde.split(';')[-1]

            if not self.multiple_values_logged:
                log_issue(logger, QA_LEVEL.WARNING,
                          Issue(QA_CHECK.Value_1_1_reference, nummeraanduiding, None, 'ligt_in_bag_woonplaats'))
                self.multiple_values_logged = True

    def enrich_verblijfsobject(self, verblijfsobject):
        if verblijfsobject.get('fng_code') is None:
            return

        # Get the omschrijving for the finiancieringscode
        verblijfsobject['fng_omschrijving'] = FINANCIERINGSCODE_MAPPING.get(str(verblijfsobject['fng_code']))

        if verblijfsobject['pandidentificatie']:
            verblijfsobject['pandidentificatie'] = verblijfsobject['pandidentificatie'].split(";")
