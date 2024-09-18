# TODO DRY up with power-ops-functions

from typing import Any, Optional, TypeVar, overload

_T = TypeVar("_T")


@overload
def require(value: Optional[_T]) -> _T: ...


@overload
def require(value: Any, as_type: type[_T]) -> _T: ...


def require(value: Any, as_type: Any = None) -> Any:
    if value is None:
        raise ValueError("Value is required")
    if as_type is not None and not isinstance(value, as_type):
        raise TypeError(f"Expected type '{as_type}', got '{type(value)}'")
    return value
