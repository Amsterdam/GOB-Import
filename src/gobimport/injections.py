"""Injections.

Conversion logic to populate a source with fixed data.

This can be used when importing data from new sources.
The data in the new source can be populated so that it joins the previous source.

ToDo: handle data that has states (key consists of multiple items).
"""


import json


class Injector:
    """Inject a data source with fixed data."""

    def __init__(self, inject_spec):
        """Initialise Injector."""
        self.inject_spec = inject_spec

        if inject_spec:
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
        """Inject data row."""
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
        """Apply an injection.

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
