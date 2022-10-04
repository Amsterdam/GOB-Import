"""Validator.

Basic validation logic.

The first version implements only the most basic validation logic.
In order to prepare a more generic validation approach the validation has been set up
by means of regular expressions.
"""

import re

from gobcore.exceptions import GOBException
from gobcore.model.metadata import FIELD
from gobcore.logging.logger import logger
from gobcore.quality.issue import QA_CHECK, QA_LEVEL, Issue, log_issue

from gobimport import gob_model
from gobimport.utils import get_nested_item, split_field_reference


# Log message formats
MISSING_ATTR_FMT = "{attr} missing in entity: {entity}"
QA_CHECK_FAILURE_FMT = "{msg}. Value was: {value}"


ENTITY_CHECKS = {
    "test_entity": {},
    "meetbouten": {
        "meetbouten": {
            "identificatie": [
                {
                    **QA_CHECK.Format_N8,
                    "level": QA_LEVEL.FATAL,
                },
            ],
            "status.code": [
                {
                    **QA_CHECK.Value_1_2_3,
                    "level": QA_LEVEL.WARNING,
                },
            ],
            "windrichting": [
                {
                    **QA_CHECK.Value_wind_direction_NOZW,
                    "allow_null": True,
                    "level": QA_LEVEL.WARNING,
                }
            ],
            "publiceerbaar": [
                {
                    **QA_CHECK.Is_boolean,
                    "allow_null": True,
                    "level": QA_LEVEL.WARNING,
                },
            ],
        },
        "metingen": {
            "identificatie": [
                {
                    **QA_CHECK.Format_numeric,
                    "level": QA_LEVEL.FATAL,
                },
            ],
            "hoort_bij_meetbouten_meetbout.bronwaarde": [
                {
                    **QA_CHECK.Format_N8,
                    "level": QA_LEVEL.FATAL,
                },
            ],
            "publiceerbaar": [
                {
                    **QA_CHECK.Is_boolean,
                    "allow_null": True,
                    "level": QA_LEVEL.WARNING,
                },
            ],
        },
        "referentiepunten": {
            "publiceerbaar": [
                {
                    **QA_CHECK.Is_boolean,
                    "allow_null": True,
                    "level": QA_LEVEL.FATAL,
                },
            ],
        },
        "rollagen": {
            "identificatie": [
                {
                    **QA_CHECK.Format_AANN,
                    "level": QA_LEVEL.WARNING,
                },
            ],
        }
    },
    "nap": {
        "peilmerken": {
            "identificatie": [
                {
                    **QA_CHECK.Format_N8,
                    "level": QA_LEVEL.FATAL,
                },
            ],
            "hoogte_tov_nap": [
                {
                    **QA_CHECK.Value_height_6_15,
                    "level": QA_LEVEL.WARNING,
                },
            ],
            "geometrie": [
                {
                    **QA_CHECK.Value_geometry_in_NL,
                    "level": QA_LEVEL.FATAL,
                },
            ],
            "publiceerbaar": [
                {
                    **QA_CHECK.Is_boolean,
                    "allow_null": True,
                    "level": QA_LEVEL.FATAL,
                },
            ]
        }
    },
    "gebieden": {
        "bouwblokken": {
            "_source_id": [
                {
                    "source_app": "DGDialog",
                    **QA_CHECK.Format_4_2_2_2_6_HEX_SEQ,
                    "level": QA_LEVEL.WARNING
                }
            ],
            "code": [
                {
                    **QA_CHECK.Format_AANN,
                    "level": QA_LEVEL.FATAL
                }
            ],
        },
        "buurten": {
            "geometrie": [
                {
                    **QA_CHECK.Value_geometry_in_NL,
                    "level": QA_LEVEL.FATAL,
                },
            ],
            "naam": [
                {
                    **QA_CHECK.Value_not_empty,
                    "level": QA_LEVEL.WARNING
                }
            ],
        },
        "wijken": {
            "code": [
                {
                    **QA_CHECK.Value_not_empty,
                    "level": QA_LEVEL.FATAL
                }
            ]
            # Temporary fix, turn back on when values will be filled again.
            # "documentnummer": [
            #     {
            #         **QA_CHECK.Value_not_empty,
            #         "level": QA_LEVEL.WARNING,
            #     },
            # ],
        },
        "ggpgebieden": {
            "naam": [
                {
                    **QA_CHECK.Format_alphabetic,
                    "level": QA_LEVEL.WARNING,
                },
            ],
        },
        "ggwgebieden": {
            "naam": [
                {
                    **QA_CHECK.Format_alphabetic,
                    "level": QA_LEVEL.WARNING,
                },
            ],
        },
        "stadsdelen": {
            "naam": [
                {
                    **QA_CHECK.Format_alphabetic,
                    "level": QA_LEVEL.WARNING,
                },
            ],
        }
    },
    "bag": {
        "brondocumenten": {
            "documentnummer": [
                {
                    **QA_CHECK.Value_brondocument_coding,
                    "level": QA_LEVEL.WARNING,
                }
            ]
        },
        "verblijfsobjecten": {
            "toegang": [
                {
                    "source_app": "Neuron",
                    **QA_CHECK.Value_not_empty,
                    "level": QA_LEVEL.WARNING
                },
            ],
            "aantal_bouwlagen": [
                {
                    "source_app": "Neuron",
                    **QA_CHECK.Value_not_empty,
                    "level": QA_LEVEL.WARNING
                },
            ],
            "verdieping_toegang": [
                {
                    "source_app": "Neuron",
                    **QA_CHECK.Value_not_empty,
                    "level": QA_LEVEL.WARNING
                },
            ],
            "redenopvoer.omschrijving": [
                {
                    "source_app": "Neuron",
                    **QA_CHECK.Value_not_empty,
                    "level": QA_LEVEL.WARNING
                },
            ],
        },
        "openbareruimtes": {
            # "ligt_in_woonplaats": [
            #     {
            #         "source_app": "Neuron",
            #         **QA_CHECK.Value_woonplaats_bronwaarde_1012_1024_1025_3594,
            #         "level": QA_LEVEL.FATAL
            #     }
            # ],
            "geometrie": [
                {
                    "source_app": "Neuron",
                    **QA_CHECK.Value_not_empty,
                    "level": QA_LEVEL.WARNING
                },
            ],
        },
        "ligplaatsen": {
        },
        "standplaatsen": {
        },
        "woonplaatsen": {
            # "identificatie": [
            #     {
            #         "source_app": "Neuron",
            #         **QA_CHECK.Value_woonplaats_1012_1024_1025_3594,
            #         "level": QA_LEVEL.FATAL
            #     }
            # ],
        },
        "nummeraanduidingen": {
            # "ligt_in_woonplaats": [
            #     {
            #         "source_app": "Neuron",
            #         **QA_CHECK.Value_woonplaats_bronwaarde_1012_1024_1025_3594,
            #         "level": QA_LEVEL.FATAL
            #     }
            # ],
        },
        "panden": {
        }
    }
}


class Validator:

    def __init__(self, source_app, catalogue, entity_name, input_spec):
        self.source_app = source_app
        self.catalogue = catalogue
        self.entity_name = entity_name
        self.entity_id = gob_model[self.catalogue]['collections'][self.entity_name].get('entity_id')
        self.input_spec = input_spec

        self.qa_checks = ENTITY_CHECKS.get(catalogue, {}).get(entity_name, {})
        self.collection_qa = {f"num_invalid_{attr}": 0 for attr in self.qa_checks.keys()}
        self.fatal = False

        self.primary_keys = set()
        self.duplicates = set()

        self.validate_functions = {
            'boolean': self._is_boolean,
            'regex': self._regex_check,
            'between': self._between_check,
            'geometry': self._geometry_check,
        }

    def result(self):
        if self.fatal:
            raise GOBException(
                f"Quality assurance failed for {self.entity_name}"
            )

        if self.duplicates:
            raise GOBException(f"Duplicate primary key(s) found in source: "
                               f"[{', '.join([str(dup) for dup in self.duplicates])}]")

        logger.info("Quality assurance passed")

    def validate(self, entity):
        """Validate a single entity.

        :param self:
        :return:
        """
        # Validate uniqueness of primary key
        self._validate_primary_key(entity)

        # Run quality checks on the collection and individual entities
        self._validate_quality(entity)

    def _validate_primary_key(self, entity):
        """Validate a primary key.

        Primary key should be unique for source_id + seqnr if the collection has states.

        :param entity:
        :return:
        """
        entity_source_id = entity[FIELD.SOURCE_ID]

        if entity_source_id is not None:
            # Only add ids that are not None, None id's can occur for imports of collections without ids
            if entity_source_id not in self.primary_keys:
                self.primary_keys.add(entity_source_id)
            else:
                self.duplicates.add(entity_source_id)

    def _validate_entity(self, entity):
        """Validate a single entity.

        Fails on any fatal validation check.
        Warns on any warning validation check.
        All info validation checks are counted.

        :param entity_name: the entity name
        :param entity: a single entity
        :return: Result of the qa checks
        """
        invalid_attrs = set()
        for attr, entity_checks in self.qa_checks.items():
            for check in entity_checks:
                if check.get("source_app", self.source_app) != self.source_app:
                    # Checks can be made app specific by setting the source_app attribute
                    continue
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
        level = check["level"]
        # Check if (nested) attr is available in entity
        key_list = split_field_reference(attr)
        _current_level = entity
        for key in key_list:
            if key in _current_level:
                _current_level = _current_level[key]
            else:
                # If a fatal check has failed, mark the validation as fatal
                if level == QA_LEVEL.FATAL:
                    self.fatal = True

                log_issue(logger, level, Issue(QA_CHECK.Attribute_exists, entity, self.entity_id, attr))
                return False
        return True

    def _qa_check(self, check, attr, entity):
        level = check["level"]
        key_list = split_field_reference(attr)
        value = get_nested_item(entity, *key_list)

        validate_function = self.validate_functions.get(check['type'])
        is_correct = validate_function(check, value)

        # If the value doesn't pass the qa check, handle the correct way
        if not is_correct:
            # If a fatal check has failed, mark the validation as fatal
            if level == QA_LEVEL.FATAL:
                self.fatal = True
            log_issue(logger, level, Issue(check, entity, self.entity_id, attr))
            return False

        return True

    def _is_boolean(self, check, value):
        # Check if Null values are allowed else return true if value is a boolean.
        allow_null = check.get('allow_null')
        if allow_null and value is None:
            return True
        return isinstance(value, bool)

    def _regex_check(self, check, value):
        # Check if Null values are allowed
        allow_null = check.get('allow_null')
        if allow_null and value is None:
            return True
        if not allow_null and value is None:
            return False
        return re.compile(check['pattern']).match(str(value))

    def _between_check(self, check, value):
        values = check.get('values')
        assert values, 'Between values should be configured for this check'
        return values[0] <= float(value) <= values[1] if value is not None else False

    def _geometry_check(self, check, value):
        values = check.get('values')
        assert values, 'Geometry values should be configured for this check'
        coords = re.findall(r'([0-9]+\.[0-9]+)', value)
        # Loop through all coords and check if they fill within the supplied range.
        # Even coords are x values, uneven are y values
        coord_types = ['x', 'y']
        for count, coord in enumerate(coords):
            # Get the coord type
            coord_type = coord_types[count % 2]
            # If the coord is outside of the boundaries, retun false
            if not values[coord_type]['min'] <= float(coord) <= values[coord_type]['max']:
                return False
        return True

    def _validate_quality(self, entity):
        """Validate an entity.

        Fails on any fatal validation check.
        Warns on any warning validation check.
        All info validation checks are counted.

        :param entity_name: the entity name
        :param entity: a single entity
        :param source_id: the column defining the unique identifier
        :return: Result of the qa checks, and a boolean if fatal errors have been found
        """
        # Validate on individual entities
        invalid_attrs = self._validate_entity(entity)
        for attr in invalid_attrs:
            self.collection_qa[f"num_invalid_{attr}"] += 1
