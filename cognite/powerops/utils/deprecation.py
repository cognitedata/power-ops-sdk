import warnings
from typing import TypeVar

T = TypeVar("T")
_WARNED_CLASSES: set[str] = set()


def deprecated_class(cls: type[T]) -> type[T]:
    """Decorator to mark a class as deprecated, showing a message only once."""

    # Intercept method calls
    def new_getattribute(self, name):  # type: ignore[no-untyped-def]
        class_name = cls.__name__
        if class_name not in _WARNED_CLASSES:
            warnings.warn(
                f"\nWarning: Class `{cls.__name__}` is deprecated and will be removed.",
                DeprecationWarning,
                stacklevel=2,
            )
            _WARNED_CLASSES.add(class_name)
        return super(cls, self).__getattribute__(name)

    cls.__getattribute__ = new_getattribute  # type: ignore[method-assign]

    return cls
