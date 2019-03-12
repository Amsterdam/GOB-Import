import datetime
import decimal


def _enrich_metingen(entities):     # noqa: C901
    """
    Enrich a metingen dataset.

    :param entities: a metingen dataset
    :return: None
    """
    update_attributes = [
        'type_meting',
        'hoeveelste_meting',
        'aantal_dagen',
        'zakking',
        'zakking_cumulatief',
        'zakkingssnelheid'
    ]

    # Keep a dict of metingen by meetboutid
    meetbouten = {}
    for entity in entities:
        meetboutid = entity['hoort_bij_meetbout']

        huidige_datum = datetime.datetime.strptime(entity['datum'], '%Y-%m-%d')

        try:
            meetbout = meetbouten[meetboutid]
            # If this meetbout has been measured before it is a 'Herhaalmeting', and update the count
            meetbout['type_meting'] = 'H'
            meetbout['hoeveelste_meting'] = meetbout['hoeveelste_meting'] + 1
        except KeyError:
            meetbout = meetbouten[meetboutid] = {
                'type_meting': 'N',
                'hoeveelste_meting': 1,
                'aantal_dagen': 0,
                'zakking': 0,
                'zakking_cumulatief': 0,
                'zakkingssnelheid': 0,
                '_eerste_datum': huidige_datum,
                '_vorige_datum': huidige_datum,
                '_vorige_hoogte': entity['hoogte_tov_nap']
            }

        # Calculate number of days since previous meting
        meetbout['aantal_dagen'] = _calculate_days_since(meetbout['_vorige_datum'], huidige_datum)

        # Store the value for the next interation
        meetbout['_vorige_datum'] = huidige_datum

        # Calculate zakking since previous meting
        meetbout['zakking'] = _calculate_zakking(meetbout['_vorige_hoogte'], entity['hoogte_tov_nap'])
        meetbout['zakking_cumulatief'] += meetbout['zakking']

        # Store the value for the next interation
        meetbout['_vorige_hoogte'] = entity['hoogte_tov_nap']

        # Calculate zakkingssnelheid
        aantal_dagen_sinds_eerste_meting = _calculate_days_since(meetbout['_eerste_datum'], huidige_datum)
        meetbout['zakkingssnelheid'] = _calculate_zakkingssnelheid(meetbout['zakking_cumulatief'],
                                                                   aantal_dagen_sinds_eerste_meting)

        for attr in update_attributes:
            entity[attr] = meetbout[attr]

        # Convert refereert_aan_refpunt to JSON
        try:
            entity['refereert_aan_refpunt'] = entity['refereert_aan_refpunt'].split(';')
        except (KeyError, AttributeError) as e:
            pass


def _calculate_days_since(previous_date, current_date):
    """
    Calculate the number of days between two dates

    :param previous_date: datetime object
    :param current_date: datetime object
    :return: number of days
    """
    delta = current_date - previous_date
    return delta.days


def _calculate_zakking(previous_value, current_value):
    """
    Calculate the amount of zakking between two metingen

    :param previous_value: float value in m
    :param current_value: float value in m
    :return: the amount of zakking in mm
    """
    return (previous_value * 1000) - (current_value * 1000)


def _calculate_zakkingssnelheid(zakking, aantal_dagen):
    """
    Calculate the zakkingssnelheid in mm/j based on the zakking and the amount of days past

    :param zakking: float value in mm
    :param aantal_dagen: int value
    :return: the amount of zakking per year in mm/j
    """
    try:
        zakkingssnelheid = decimal.Decimal(str(365 / aantal_dagen)) * decimal.Decimal(str(zakking))
    except ZeroDivisionError:
        zakkingssnelheid = 0
    return zakkingssnelheid
