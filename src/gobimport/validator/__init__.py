""" Validator

Basic validation logic

The first version implements only the most basic validation logic
In order to prepare a more generic validation approach the validation has been set up by means of regular expressions.

"""
from enum import Enum
import re

from gobcore.exceptions import GOBException


# Log message formats
MISSING_ATTR_FMT = "{attr} missing in entity: {entity}"
QA_CHECK_FAILURE_FMT = "{msg}. Value was: {value}"


class QA(Enum):
    FATAL = "fatal"
    WARNING = "warning"
    INFO = "info"


ENTITY_CHECKS = {
    "meetbouten": {
        "meetboutid": [
            {
                "pattern": "^\d{8}$",
                "msg": "Meetboutid should consist of 8 numeric characters",
                "type": QA.FATAL,
            },
        ],
        "status_id": [
            {
                "pattern": "^[1,2,3]$",
                "msg": "Statusid should be one of [1,2,3].",
                "type": QA.WARNING,
            },
        ],
        "windrichting": [
            {
                "pattern": "^(N|NO|O|ZO|Z|ZW|W|NW)$",
                "msg": "Windrichting should be one of [N,NO,O,ZO,Z,ZW,W,NW]",
                "type": QA.WARNING,
            }
        ],
        "publiceerbaar": [
            {
                "pattern": "^[J,N]$",
                "msg": "Publiceerbaar should be one of [J,N]",
                "type": QA.FATAL,
            },
        ],
    },
    "metingen": {
        "metingid": [
            {
                "pattern": "^\d+$",
                "msg": "Metingid should be a valid positive integer",
                "type": QA.FATAL,
            },
        ],
        "hoort_bij_meetbout": [
            {
                "pattern": "^\d{8}$",
                "msg": "Meetboutid should consist of 8 numeric characters",
                "type": QA.FATAL,
            },
        ],
    },
    "referentiepunten": {},
    "rollagen": {
        "name": [
            {
                "pattern": "^[a-zA-Z]{2}\d{1,2}",
                "msg": "File name should be in the format AAn(n)",
                "type": QA.FATAL,
            },
        ],
    },
}


class Validator:

    def __init__(self, import_client, entity_name, entities, source_id):
        # Save a reference to import_client to use logging function
        self.import_client = import_client

        self.entity_name = entity_name
        self.entities = entities
        self.source_id = source_id

        self.collection_qa = {f"num_invalid_{attr}": 0 for attr in ENTITY_CHECKS[entity_name].keys()}
        self.collection_qa['num_records'] = len(entities)
        self.fatal = False

    def validate(self):
        """
        Validate each entity in the list of entities for a given entity name

        :param self:
        :return:
        """

        # Validate uniqueness of primary key
        self._validate_primary_key()

        # Run quality checks on the collection and individual entities
        self._validate_quality()

        if self.fatal:
            raise GOBException(
                f"Quality assurance failed for {self.entity_name} from source {self.import_client.source['name']}"
            )

        self._log(QA.INFO, f"Quality assurance passed", self.collection_qa)

    def _log(self, type, msg, data=None):
        if type == QA.FATAL:
            self.import_client.log("error", msg, data)
        if type == QA.WARNING:
            self.import_client.log("warning", msg, data)
        if type == QA.INFO:
            self.import_client.log("info", msg, data)

    def _validate_primary_key(self):
        primary_keys = set()
        duplicates = set()
        for entity in self.entities:
            if entity[self.source_id] not in primary_keys:
                primary_keys.add(entity[self.source_id])
            else:
                duplicates.add(entity[self.source_id])
        if duplicates:
            raise GOBException(f"Duplicate primary key(s) found in source: [{', '.join(duplicates)}]")

    def _validate_entity(self, entity):
        """
        Validate a single entity.

        Fails on any fatal validation check
        Warns on any warning validation check
        All info validation checks are counted

        :param entity_name: the entity name
        :param entity: a single entity
        :return: Result of the qa checks
        """
        qa_checks = ENTITY_CHECKS[self.entity_name]

        invalid_attrs = set()

        for attr, entity_checks in qa_checks.items():
            for check in entity_checks:
                # Check if the attribute is available
                if not self._attr_check(check, attr, entity):
                    # Add the attribute to the set of non-valid attributes for count
                    invalid_attrs.add(attr)
                    continue

                if not self._qa_check(check, attr, entity):
                    # Add the attribute to the set of non-valid attributes for count
                    invalid_attrs.add(attr)

        return invalid_attrs

    def _attr_check(self, check, attr, entity):
        type = check["type"]
        # Check if attr is available in entity
        if attr not in entity:
            # If a fatal check has failed, mark the validation as fatal
            if type == QA.FATAL:
                self.fatal = True

            # Log the missing attribute, with the correct level
            self._log(type, MISSING_ATTR_FMT.format(attr=attr, entity=entity[self.source_id]),
                      {self.source_id: entity[self.source_id], "missing_attr": attr})

            return False

        return True

    def _qa_check(self, check, attr, entity):
        pattern = check["pattern"]
        msg = check["msg"]
        type = check["type"]
        value = str(entity[attr])

        # If the value doesn't match the pattern, handle the correct way
        if not re.compile(pattern).match(value):
            # If a fatal check has failed, mark the validation as fatal
            if type == QA.FATAL:
                self.fatal = True

            # Log the missing attribute, with the correct level
            self._log(type, QA_CHECK_FAILURE_FMT.format(msg=msg, value=value),
                      {self.source_id: entity[self.source_id], "value": value})

            return False

        return True

    def _validate_quality(self):
        """
        Validate a list of entities.

        Fails on any fatal validation check
        Warns on any warning validation check
        All info validation checks are counted

        :param entity_name: the entity name
        :param entity: a single entity
        :param source_id: the column defining the unique identifier
        :return: Result of the qa checks, and a boolean if fatal errors have been found
        """
        # Validate on individual entities
        for entity in self.entities:
            invalid_attrs = self._validate_entity(entity)
            for attr in invalid_attrs:
                self.collection_qa[f"num_invalid_{attr}"] += 1