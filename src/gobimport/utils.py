from functools import reduce
from typing import Any


def split_field_reference(ref: str) -> list[str]:
    """Split field reference in an array."""
    return ref.split(".") if "." in ref else [ref]


def get_nested_item(data: dict[str, Any], *keys: str):
    """Get a nested item from a dictionary.

    Example: get_nested_item({'a': {'b': {'c': 5}}}, 'a', 'b', 'c') = 5
    The function eliminates ugly code like dict.get('a', {}).get('b', {}).get('c')

    :param data:
    :param keys:
    :return:
    """
    return reduce(lambda d, key: d.get(key, None) if isinstance(d, dict) else None, keys, data)
