""" Entity validation

Validation will take place after the imported data has been converted into the GOBModel.
This is done to be able to perform comparisons between dates in the imported data or
run specific validation for certain collections.
"""
from collections import defaultdict

from gobcore.exceptions import GOBException
from gobcore.model import GOBModel
from gobcore.logging.logger import logger

from gobimport.entity_validator.gebieden import _validate_bouwblokken, _validate_buurten


def entity_validate(catalogue, entity_name, entities, source_id):
    """
    Validate each entity in the list of entities for a given entity name

    :param catalogue: the name of the catalogue, e.g. meetbouten
    :param entity_name: the name of the entity, e.g. meetbouten
    :param entities: the list of entities
    :return:
    """
    model = GOBModel()

    # if model has state, run validations for checks with state
    states_validated = not model.has_states(catalogue, entity_name) or _validate_entity_state(entities, source_id)

    validators = {
        "bouwblokken": _validate_bouwblokken,
        "buurten": _validate_buurten,
    }

    entities_validated = validators.get(entity_name, lambda *args: True)(entities, source_id)

    # Raise an Exception is a fatal validation has failed
    if not (entities_validated and states_validated):
        raise GOBException(
            f"Quality assurance failed for {entity_name}"
        )


def _validate_entity_state(entities, source_id):
    """
    Validate entitys with state to see if generic validations for states are correct.

    Checks that are being performed:

    - begin_geldigheid should not be after eind_geldigheid (when filled)
    - volgnummer should be a positive number and unique in the collection

    :param entities: the list of entities
    :return:
    """
    validated = True

    volgnummers = defaultdict(set)
    end_date = {}

    for entity in entities:
        validated = _validate_begin_geldigheid(entity, source_id)

        # volgnummer should a positive number and unique in the collection
        volgnummer = str(entity['volgnummer'])
        identificatie = str(entity[source_id])
        if int(volgnummer) < 1 or volgnummer in volgnummers[identificatie]:
            msg = "volgnummer should be a positive number and unique in the collection"
            extra_data = {
                'id': msg,
                'data': {
                    'identificatie': entity[source_id],
                    'volgnummer': entity['volgnummer'],
                }
            }
            logger.error(msg, extra_data)
            validated = False

        # Only one eind_geldigheid may be empty per entity
        eind_geldigheid = entity['eind_geldigheid']
        if eind_geldigheid is None:
            if end_date.get(identificatie):
                msg = "Only one eind_geldigheid for every entity may be empty"
                extra_data = {
                    'id': msg,
                    'data': {
                        'identificatie': entity[source_id],
                    }
                }
                logger.warning(msg, extra_data)
            end_date[identificatie] = True

        # Add the volgnummer to the set for this entity identificatie
        volgnummers[identificatie].add(volgnummer)

    return validated


def _validate_begin_geldigheid(entity, source_id):
    validated = True

    # begin_geldigheid should be filled
    if not entity['begin_geldigheid']:
        msg = "begin_geldigheid should be filled"
        extra_data = {
            'id': msg,
            'data': {
                'identificatie': entity[source_id],
                'begin_geldigheid': entity['begin_geldigheid'].isoformat(),
            }
        }
        logger.error(msg, extra_data)
        validated = False

    # begin_geldigheid can not be after eind_geldigheid when filled
    if entity['eind_geldigheid'] and \
       entity['eind_geldigheid'] < entity['begin_geldigheid']:
        msg = "begin_geldigheid can not be after eind_geldigheid"
        extra_data = {
            'id': msg,
            'data': {
                'identificatie': entity[source_id],
                'begin_geldigheid': entity['begin_geldigheid'].isoformat(),
                'eind_geldigheid': entity['eind_geldigheid'].isoformat(),
            }
        }
        logger.warning(msg, extra_data)

    return validated
