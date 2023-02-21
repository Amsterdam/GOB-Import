"""BAG enrichment."""


from typing import Any

from gobcore.logging.logger import logger
from gobcore.quality.issue import QA_CHECK, QA_LEVEL, Issue, log_issue

from gobimport.enricher.enricher import Enricher

CODE_TABLE_FIELDS = ["code", "omschrijving"]

FINANCIERINGSCODE_MAPPING = {
    "501": "Eigen bouw (501)",
    "110": "Sociale huursector woningwet (110)",
    "200": "Premiehuur Profit (200)",
    "201": "Premiehuur Profit voor 1985 (201)",
    "220": "Wet Materiële Oorlogsschade (220)",
    "250": "Huurwoningen Beleggers (250)",
    "271": "Premiehuur Profit met gemeentegarantie (271)",
    "274": "Sociale koop (voorheen Premiekoop A) (274)",
    "301": "Premiekoop voor 1985 (301)",
    "373": "Subsidie Premiekoop A (373)",
    "374": "Subsidie Premiekoop B (374)",
    "375": "Premiewoningen (375)",
    "466": "V.S.E.B. (466)",
    "475": "V.S.E.B. Premiekoop C (475)",
    "476": "V.S.E.B. Premiehuur C (476)",
    "500": "Ongesubsidieerde bouw (500)",
    "999": "DEFAULT VOOR CONVERSIE",
    "477": "Middeldure huur (477)",
    "478": "Middeldure koop (478)",
}


class BAGEnricher(Enricher):
    """BAG Enricher."""

    @classmethod
    def enriches(cls, app_name: str, catalog_name: str, entity_name: str) -> bool:
        """Enrich BAG collections."""
        if catalog_name == "bag":
            enricher = BAGEnricher(app_name, catalog_name, entity_name)
            return enricher._enrich_entity is not None
        return False

    def __init__(self, app_name: str, catalogue_name: str, entity_name: str) -> None:
        """Initialise BAGEnricher."""
        self.multiple_values_logged = False
        self.entity_name = entity_name

        super().__init__(
            app_name,
            catalogue_name,
            entity_name,
            methods={
                "nummeraanduidingen": self.enrich_nummeraanduiding,
                "verblijfsobjecten": self.enrich_verblijfsobject,
            },
        )

    def enrich(self, entity: dict[str, Any]) -> None:
        """Enrich a single entity, override with default enrichment for all BAG collections.

        :param entity:
        :return:
        """
        if self._enrich_entity:
            self._enrich_entity(entity)

    def enrich_nummeraanduiding(self, nummeraanduiding: dict[str, str]) -> None:
        """Enrich BAG nummeraanduiding (legacy?)."""
        # ligt_in_woonplaats can have multiple values, use the last value and log a warning
        bronwaarde = nummeraanduiding.get("ligt_in_bag_woonplaats")
        if bronwaarde and ";" in bronwaarde:
            nummeraanduiding["ligt_in_bag_woonplaats"] = bronwaarde.split(";")[-1]

            if not self.multiple_values_logged:
                log_issue(
                    logger,
                    QA_LEVEL.WARNING,
                    Issue(QA_CHECK.Value_1_1_reference, nummeraanduiding, None, "ligt_in_bag_woonplaats"),
                )
                self.multiple_values_logged = True

    def enrich_verblijfsobject(self, verblijfsobject: dict[str, Any]) -> None:
        """Enrich BAG verblijfsobject."""
        if verblijfsobject.get("fng_code") is None:
            return

        # Get the omschrijving for the finiancieringscode
        verblijfsobject["fng_omschrijving"] = FINANCIERINGSCODE_MAPPING.get(str(verblijfsobject["fng_code"]))

        if verblijfsobject["pandidentificatie"]:
            verblijfsobject["pandidentificatie"] = verblijfsobject["pandidentificatie"].split(";")
