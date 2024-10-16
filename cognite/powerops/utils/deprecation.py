import warnings
from typing import TypeVar

T = TypeVar("T")


def deprecated_class(cls: type[T]) -> type[T]:
    """Decorator to mark a class as deprecated, showing a message only once."""

    def new_getattribute(self, name):  # type: ignore[no-untyped-def]
        warnings.warn(
            f"\nWarning: Class `{cls.__name__}` is deprecated and will be removed.",
            DeprecationWarning,
            stacklevel=2,
        )
        return super(cls, self).__getattribute__(name)

    cls.__getattribute__ = new_getattribute  # type: ignore[method-assign]

    return cls
