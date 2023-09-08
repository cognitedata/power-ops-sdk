from typing import Any


def isinstance_list(value: Any, type_: type):
    return isinstance(value, list) and value and isinstance(value[0], type_)
