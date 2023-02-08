from collections import defaultdict

from gobcore.logging.logger import logger
from gobcore.model import FIELD
from gobcore.quality.issue import QA_CHECK, QA_LEVEL, Issue, log_issue

from gobimport import gob_model


class StateValidator:
    """State Validator."""

    @classmethod
    def validates(cls, catalog_name, entity_name):
        """Tell wether this class validates the given catalog entity.

        :param catalog_name:
        :param entity_name:
        :return:
        """
        return gob_model.has_states(catalog_name, entity_name)

    def __init__(self, catalog_name, entity_name, source_id):
        """Initialise StateValidator."""
        self.source_id = source_id

        self.validated = True
        self.volgnummers = defaultdict(set)
        self.end_date = {}

    def result(self):
        """Return StateValidator result."""
        return self.validated

    def validate(self, entity, merged: bool = False):
        """Validate entity with state to see if generic validations for states are correct.

        Checks that are being performed:

        - begin_geldigheid should not be after eind_geldigheid (when filled)
        - volgnummer should be a positive number and unique in the collection

        When `entity` is merged, return early because this is entity is already checked.

        :param entity: a GOB entity
        :param merged: bool
        :return:
        """
        self._validate_begin_geldigheid(entity)
        self._validate_volgnummer(entity)
        identificatie = str(entity[self.source_id])

        if merged:
            # in case Merger.prepare set this id to True and the current entity has end validity yes/no
            self.end_date[identificatie] = entity[FIELD.END_VALIDITY] is None
            return

        if entity[FIELD.SEQNR] in self.volgnummers[identificatie]:
            log_issue(logger, QA_LEVEL.ERROR, Issue(QA_CHECK.Value_unique, entity, self.source_id, FIELD.SEQNR))
            self.validated = False

        self.volgnummers[identificatie].add(entity[FIELD.SEQNR])

        # Only one eind_geldigheid may be empty per entity (non-merged)
        if entity[FIELD.END_VALIDITY] is None:
            if self.end_date.get(identificatie):
                log_issue(
                    logger,
                    QA_LEVEL.WARNING,
                    Issue(QA_CHECK.Value_empty_once, entity, self.source_id, FIELD.END_VALIDITY),
                )
            self.end_date[identificatie] = True

    def _validate_volgnummer(self, entity):
        # Volgnummer can't be empty -> Fatal
        if entity[FIELD.SEQNR] is None:
            log_issue(logger, QA_LEVEL.FATAL, Issue(QA_CHECK.Value_not_empty, entity, self.source_id, FIELD.SEQNR))
            self.validated = False

        # volgnummer should a positive number and unique in the collection
        elif entity[FIELD.SEQNR] < 1:
            log_issue(logger, QA_LEVEL.ERROR, Issue(QA_CHECK.Format_numeric, entity, self.source_id, FIELD.SEQNR))
            self.validated = False

    def _validate_begin_geldigheid(self, entity):
        if entity[FIELD.START_VALIDITY]:
            if entity[FIELD.END_VALIDITY] and entity[FIELD.START_VALIDITY] > entity[FIELD.END_VALIDITY]:
                # Start-Validity cannot be after End-Validity
                log_issue(
                    logger,
                    QA_LEVEL.WARNING,
                    Issue(
                        QA_CHECK.Value_not_after,
                        entity,
                        self.source_id,
                        FIELD.START_VALIDITY,
                        compared_to=FIELD.END_VALIDITY,
                    ),
                )
        else:
            log_issue(
                logger, QA_LEVEL.ERROR, Issue(QA_CHECK.Value_not_empty, entity, self.source_id, FIELD.START_VALIDITY)
            )
            self.validated = False
