"""Injections

Conversion logic to populate a source with fixed data

This van be used when importing data from new sources
The data in the new source can be populated so that it joins the previous source

ToDo: handle data that has states (key consists of multiple items)
"""
import json

from gobcore.logging.logger import logger


class Injector:

    def __init__(self, inject_spec):
        self.inject_spec = inject_spec

        if inject_spec:
            logger.info("Inject conversion data")

            # {
            #     "from": "<input file name>",
            #     "on": "<fieldname of field that relates the two sources>",
            #     "conversions": {
            #         "fieldname": "<operator>",
            #         ...
            #     }
            # }

            inject_from = inject_spec["from"]
            self.inject_on = inject_spec["on"]
            self.conversions = inject_spec["conversions"]

            # [
            #     {
            #         "<fieldname of field that relates the two sources>" : "<key value>",
            #         "<any fieldname>" : "<any value>",
            #         ...
            #     }, ...
            # ]
            with open(inject_from) as file:
                injections = json.load(file)

            # Convert injections into dict for fast access
            self.injections = {injection[self.inject_on]: injection for injection in injections}

    def inject(self, row):
        # {
        #     "<key value>": {
        #         "<any fieldname>" : "<any value>",
        #     }, ...
        # }
        if not self.inject_spec:
            return

        # Process row
        inject_key = row[self.inject_on]  # e.g. data["code"]
        inject_spec = self.injections.get(inject_key)  # e.g. injections["A"]
        if not inject_spec:
            return

        for key, operator in self.conversions.items():
            self._apply(row=row, key=key, operator=operator, value=inject_spec[key])

    def _apply(self, row, key, operator, value):
        """Apply an injection

        :param row: the data row to which the injection should be applied
        :param key: the key
        :param operator: the operator to apply
        :param value: the value to apply
        :return:
        """
        # Process an injection
        if operator == "=":  # Overwrite value
            row[key] = value
        elif operator == "+":  # Add to value
            row[key] += value
        elif operator == "+-1":  # Add to value
            row[key] += value - 1


def _apply_injection(row, key, operator, value):
    """Apply an injection

    :param row: the data row to which the injection should be applied
    :param key: the key
    :param operator: the operator to apply
    :param value: the value to apply
    :return:
    """
    # Process an injection
    if operator == "=":  # Overwrite value
        row[key] = value
    elif operator == "+":  # Add to value
        row[key] += value
    elif operator == "+-1":  # Add to value
        row[key] += value - 1


def inject(inject_spec, data):
    """Inject data

    Inject data from another source into data

    :param inject_spec: filename of other source specifications
    :param data: the data to inject into
    :return: None
    """
    if not inject_spec:
        return

    # {
    #     "from": "<input file name>",
    #     "on": "<fieldname of field that relates the two sources>",
    #     "conversions": {
    #         "fieldname": "<operator>",
    #         ...
    #     }
    # }

    inject_from = inject_spec["from"]
    inject_on = inject_spec["on"]
    conversions = inject_spec["conversions"]

    # [
    #     {
    #         "<fieldname of field that relates the two sources>" : "<key value>",
    #         "<any fieldname>" : "<any value>",
    #         ...
    #     }, ...
    # ]
    with open(inject_from) as file:
        injections = json.load(file)

    # Convert injections into dict for fast access
    injections = {injection[inject_on]: injection for injection in injections}

    # {
    #     "<key value>": {
    #         "<any fieldname>" : "<any value>",
    #     }, ...
    # }

    for row in data:
        # Process each row
        inject_key = row[inject_on]  # e.g. data["code"]
        inject_spec = injections.get(inject_key)  # e.g. injections["A"]
        if not inject_spec:
            continue

        for key, operator in conversions.items():
            _apply_injection(row=row, key=key, operator=operator, value=inject_spec[key])
