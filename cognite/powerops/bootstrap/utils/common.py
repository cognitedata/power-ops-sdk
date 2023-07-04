import re
from uuid import uuid4


def replace_nordic_letters(input_string: str) -> str:
    return input_string
    # return (
    #     input_string.replace("æ", "a")
    #     .replace("Æ", "A")
    #     .replace("ø", "o")
    #     .replace("Ø", "O")
    #     .replace("å", "a")
    #     .replace("Å", "A")
    # )


def special_case_handle_gate_number(name: str) -> None:
    """Must handle any gate numbers above 1 if found in other sources than YAML"""
    # TODO: extend to handle special case if needed
    if re.search(pattern=r"L[2-9]", string=name):
        print_warning(f"Potential gate {name} not in YAML!")


def print_warning(s: str) -> None:
    """Adds some nice colors to the printed text :)"""
    print(f"\033[91m[WARNING] {s}\033[0m")


def unique_str() -> str:
    return str(uuid4())


def merge(*sources: dict) -> dict:
    """
    Merge multiple dicts:
     * Nested dicts are merged (hard type check).
     * When keys conflict, last source wins.
     * Order should be preserved. Hopefully.

    >>> merge({"a": 1, "c": 3}, {"b": 22, "c": 33})
    {'a': 1, 'c': 33, 'b': 22}

    >>> merge({"a": {"aa": 1}}, {"a": {"bb": 22}, "z": 33})
    {'a': {'aa': 1, 'bb': 22}, 'z': 33}
    """

    def _merge_two(val1, val2):
        if isinstance(val1, dict) and isinstance(val2, dict):
            return merge(val1, val2)
        else:
            return val2

    result = {}
    for source in sources:
        for key, val in source.items():
            if key in result:
                val = _merge_two(result[key], val)
            result[key] = val
    return result


def dump_cdf_resource(resource) -> dict:
    """Legacy or DM resource."""
    try:
        dump_func = resource.dump
    except AttributeError:
        dump_func = resource.dict
    return dump_func()
