"""Meetbouten enrichment."""


import datetime
import decimal
from typing import Any

from gobimport.enricher.enricher import Enricher


class MeetboutenEnricher(Enricher):
    """Meetbouten Enricher."""

    @classmethod
    def enriches(cls, app_name: str, catalog_name: str, entity_name: str) -> bool:
        """Enrich Meetbouten collections."""
        if catalog_name == "meetbouten":
            enricher = MeetboutenEnricher(app_name, catalog_name, entity_name)
            return enricher._enrich_entity is not None
        return False

    def __init__(self, app_name: str, catalogue_name: str, entity_name: str) -> None:
        """Initialise MeetboutenEnricher."""
        super().__init__(
            app_name,
            catalogue_name,
            entity_name,
            methods={
                "metingen": self.enrich_meting,
            },
        )

        # Keep a dict of metingen by meetboutid
        self.meetbouten: dict[str, Any] = {}

    def enrich_meting(self, meting: dict[str, Any]) -> None:  # noqa: C901
        """Enrich a meting.

        :param meting: a meting
        :return: None
        """
        update_attributes = [
            "type_meting",
            "hoeveelste_meting",
            "aantal_dagen",
            "zakking",
            "zakking_cumulatief",
            "zakkingssnelheid",
        ]

        meetboutid = meting["hoort_bij_meetbout"]

        huidige_datum = datetime.datetime.strptime(meting["datum"], "%Y-%m-%d")

        try:
            meetbout = self.meetbouten[meetboutid]
            # If this meetbout has been measured before it is a 'Herhaalmeting', and update the count
            meetbout["type_meting"] = "H"
            meetbout["hoeveelste_meting"] = meetbout["hoeveelste_meting"] + 1
        except KeyError:
            meetbout = self.meetbouten[meetboutid] = {
                "type_meting": "N",
                "hoeveelste_meting": 1,
                "aantal_dagen": 0,
                "zakking": 0,
                "zakking_cumulatief": 0,
                "zakkingssnelheid": 0,
                "_eerste_datum": huidige_datum,
                "_vorige_datum": huidige_datum,
                "_vorige_hoogte": meting["hoogte_tov_nap"],
            }

        # Calculate number of days since previous meting
        meetbout["aantal_dagen"] = _calculate_days_since(meetbout["_vorige_datum"], huidige_datum)

        # Store the value for the next interation
        meetbout["_vorige_datum"] = huidige_datum

        # Calculate zakking since previous meting
        meetbout["zakking"] = _calculate_zakking(meetbout["_vorige_hoogte"], meting["hoogte_tov_nap"])
        meetbout["zakking_cumulatief"] += meetbout["zakking"]

        # Store the value for the next interation
        meetbout["_vorige_hoogte"] = meting["hoogte_tov_nap"]

        # Calculate zakkingssnelheid
        aantal_dagen_sinds_eerste_meting = _calculate_days_since(meetbout["_eerste_datum"], huidige_datum)
        meetbout["zakkingssnelheid"] = _calculate_zakkingssnelheid(
            meetbout["zakking_cumulatief"], aantal_dagen_sinds_eerste_meting
        )

        for attr in update_attributes:
            meting[attr] = meetbout[attr]


def _calculate_days_since(previous_date: datetime.datetime, current_date: datetime.datetime) -> int:
    """Calculate the number of days between two dates.

    :param previous_date: datetime object
    :param current_date: datetime object
    :return: number of days
    """
    delta = current_date - previous_date
    return delta.days


def _calculate_zakking(previous_value: float, current_value: float) -> float:
    """Calculate the amount of zakking between two metingen.

    :param previous_value: float value in m
    :param current_value: float value in m
    :return: the amount of zakking in mm
    """
    return (previous_value * 1000) - (current_value * 1000)


def _calculate_zakkingssnelheid(zakking: float, aantal_dagen: int) -> decimal.Decimal:
    """Calculate the zakkingssnelheid in mm/j based on the zakking and the amount of days past.

    :param zakking: float value in mm
    :param aantal_dagen: int value
    :return: the amount of zakking per year in mm/j
    """
    zakkingssnelheid: decimal.Decimal
    try:
        zakkingssnelheid = decimal.Decimal(str(365 / aantal_dagen)) * decimal.Decimal(str(zakking))
    except ZeroDivisionError:
        zakkingssnelheid = decimal.Decimal(0)
    return zakkingssnelheid
