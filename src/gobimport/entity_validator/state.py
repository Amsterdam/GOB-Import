from collections import defaultdict

from gobcore.model import GOBModel, FIELD
from gobcore.logging.logger import logger
from gobcore.quality.config import QA_CHECK
from gobcore.quality.issue import Issue


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
        issues = self._validate_begin_geldigheid(entity)

        # volgnummer should a positive number and unique in the collection
        if entity[FIELD.SEQNR] < 1:
            issue = Issue(QA_CHECK.Format_numeric, entity, self.source_id, FIELD.SEQNR)
            issues.append(issue)
            logger.error(issue.msg(), issue.log_args())
            self.validated = False

        identificatie = str(entity[self.source_id])
        if entity[FIELD.SEQNR] in self.volgnummers[identificatie]:
            issue = Issue(QA_CHECK.Value_unique, entity, self.source_id, FIELD.SEQNR)
            issues.append(issue)
            logger.error(issue.msg(), issue.log_args())
            self.validated = False
        # Add the volgnummer to the set for this entity identificatie
        self.volgnummers[identificatie].add(entity[FIELD.SEQNR])

        # Only one eind_geldigheid may be empty per entity
        if entity[FIELD.END_VALIDITY] is None:
            if self.end_date.get(identificatie):
                issue = Issue(QA_CHECK.Value_empty_once, entity, self.source_id, FIELD.END_VALIDITY)
                issues.append(issue)
                logger.warning(issue.msg(), issue.log_args())
            self.end_date[identificatie] = True

        return issues

    def _validate_begin_geldigheid(self, entity):
        issue = None

        if entity[FIELD.START_VALIDITY]:
            if entity[FIELD.END_VALIDITY] and entity[FIELD.START_VALIDITY] > entity[FIELD.END_VALIDITY]:
                # Start-Validity cannot be after End-Validity
                issue = Issue(QA_CHECK.Value_not_after, entity, self.source_id,
                              FIELD.START_VALIDITY, compared_to=FIELD.END_VALIDITY)
                logger.warning(issue.msg(), issue.log_args())
        else:
            issue = Issue(QA_CHECK.Value_not_empty, entity, self.source_id, FIELD.START_VALIDITY)
            logger.error(issue.msg(), issue.log_args())
            self.validated = False

        return [issue] if issue else []
