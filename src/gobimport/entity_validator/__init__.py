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


def entity_validate(catalogue, entity_name, entities):
    """
    Validate each entity in the list of entities for a given entity name

    :param catalogue: the name of the catalogue, e.g. meetbouten
    :param entity_name: the name of the entity, e.g. meetbouten
    :param entities: the list of entities
    :return:
    """
    model = GOBModel()

    # if model has state, run validations for checks with state
    states_validated = _validate_entity_state(entities) if model.has_states(catalogue, entity_name) else True

    validators = {
        "bouwblokken": _validate_bouwblokken,
        "buurten": _validate_buurten,
    }

    entities_validated = validators.get(entity_name, lambda _: True)

    # Raise an Exception is a fatal validation has failed
    if not (entities_validated and states_validated):
        raise GOBException(
            f"Quality assurance failed for {entity_name}"
        )


def _validate_entity_state(entities):
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
        # begin_geldigheid can not be after eind_geldigheid when filled
        if entity['eind_geldigheid'].to_db and \
           entity['eind_geldigheid'].to_db < entity['begin_geldigheid'].to_db:
            msg = "begin_geldigheid can not be after eind_geldigheid"
            extra_data = {
                'id': msg,
                'data': {
                    'identificatie': entity['identificatie'],
                    'begin_geldigheid': entity['begin_geldigheid'],
                    'eind_geldigheid': entity['eind_geldigheid'],
                }
            }
            logger.error(msg, extra_data)
            validated = False

        # volgnummer should a positive number and unique in the collection
        volgnummer = str(entity['volgnummer'])
        identificatie = str(entity['identificatie'])
        if int(volgnummer) < 1 or volgnummer in volgnummers[identificatie]:
            msg = "volgnummer should be a positive number and unique in the collection"
            extra_data = {
                'id': msg,
                'data': {
                    'identificatie': entity['identificatie'],
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
                        'identificatie': entity['identificatie'],
                    }
                }
                logger.warning(msg, extra_data)
            end_date[identificatie] = True

        # Add the volgnummer to the set for this entity identificatie
        volgnummers[identificatie].add(volgnummer)

    return validated
