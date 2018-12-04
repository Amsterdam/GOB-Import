"""Injections

Conversion logic to populate a source with fixed data

This van be used when importing data from new sources
The data in the new source can be populated so that it joins the previous source

ToDo: handle data that has states (key consists of multiple items)
"""
import json


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
    if operator == "+":  # Add to value
        row[key] += value


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
