from collections import defaultdict

from gobcore.model import GOBModel
from gobcore.logging.logger import logger


class StateValidator:

    @classmethod
    def validates(cls, catalog_name, entity_name):
        """
        Tells wether this class validates the given catalog entity

        :param catalog_name:
        :param entity_name:
        :return:
        """
        return GOBModel().has_states(catalog_name, entity_name)

    def __init__(self, catalog_name, entity_name, source_id):
        self.source_id = source_id

        self.validated = True
        self.volgnummers = defaultdict(set)
        self.end_date = {}

    def result(self):
        return self.validated

    def validate(self, entity):
        """
        Validate entity with state to see if generic validations for states are correct.

        Checks that are being performed:

        - begin_geldigheid should not be after eind_geldigheid (when filled)
        - volgnummer should be a positive number and unique in the collection

        :param entity: a GOB entity
        :return:
        """
        self._validate_begin_geldigheid(entity)

        # volgnummer should a positive number and unique in the collection
        volgnummer = entity['volgnummer']
        identificatie = str(entity[self.source_id])
        if volgnummer < 1 or volgnummer in self.volgnummers[identificatie]:
            msg = "volgnummer should be a positive number and unique in the collection"
            extra_data = {
                'id': msg,
                'data': {
                    'identificatie': entity[self.source_id],
                    'volgnummer': entity['volgnummer'],
                }
            }
            logger.error(msg, extra_data)
            self.validated = False

        # Only one eind_geldigheid may be empty per entity
        eind_geldigheid = entity['eind_geldigheid']
        if eind_geldigheid is None:
            if self.end_date.get(identificatie):
                msg = "Only one eind_geldigheid for every entity may be empty"
                extra_data = {
                    'id': msg,
                    'data': {
                        'identificatie': entity[self.source_id],
                    }
                }
                logger.warning(msg, extra_data)
            self.end_date[identificatie] = True

        # Add the volgnummer to the set for this entity identificatie
        self.volgnummers[identificatie].add(volgnummer)

    def _validate_begin_geldigheid(self, entity):
        # begin_geldigheid should be filled
        if not entity['begin_geldigheid']:
            msg = "begin_geldigheid should be filled"
            extra_data = {
                'id': msg,
                'data': {
                    'identificatie': entity[self.source_id],
                    'begin_geldigheid': entity['begin_geldigheid'],
                }
            }
            logger.error(msg, extra_data)
            self.validated = False

        # begin_geldigheid can not be after eind_geldigheid when filled
        if entity['eind_geldigheid'] and \
                entity['eind_geldigheid'] < entity['begin_geldigheid']:
            msg = "begin_geldigheid can not be after eind_geldigheid"
            extra_data = {
                'id': msg,
                'data': {
                    'identificatie': entity[self.source_id],
                    'begin_geldigheid': entity['begin_geldigheid'].isoformat(),
                    'eind_geldigheid': entity['eind_geldigheid'].isoformat(),
                }
            }
            logger.warning(msg, extra_data)
