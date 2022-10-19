"""Entity validation.

Validation will take place after the imported data has been converted into the GOBModel.
This is done to be able to perform comparisons between dates in the imported data or
run specific validation for certain collections.
"""
from gobcore.exceptions import GOBException

from gobimport.entity_validator.state import StateValidator
from gobimport.entity_validator.gebieden import GebiedenValidator
from gobimport.entity_validator.bag import BAGValidator


class EntityValidator:

    def __init__(self, catalog_name, entity_name, source_id):
        """Select all applicable entity validators for the given catalog and entity.

        :param catalog_name:
        :param entity_name:
        :param source_id:
        """
        self.catalog_name = catalog_name
        self.entity_name = entity_name

        self.validators = []
        for Validator in [StateValidator, GebiedenValidator, BAGValidator]:
            if Validator.validates(catalog_name, entity_name):
                self.validators.append(Validator(catalog_name, entity_name, source_id))

    def validate(self, entity, **kwargs):
        """Validate the entity for all applicable validation tests.

        :param entity:
        :return: None
        """
        for validator in self.validators:
            validator.validate(entity, **kwargs)

    def result(self):
        """Checks for fatal errors.

        Any non-True result for any of the validators raises an exception.

        :return:
        """
        results = [validator.result() for validator in self.validators]
        # Raise an Exception is a fatal validation has failed
        if False in results:
            raise GOBException(
                f"Quality assurance failed for {self.catalog_name}.{self.entity_name}"
            )
        return True
