from typing import Any, Type


def isinstance_list(value: Any, type_: Type):
    return isinstance(value, list) and value and isinstance(value[0], type_)
