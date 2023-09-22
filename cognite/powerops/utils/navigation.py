import abc
import inspect
from collections.abc import Iterable, Sequence
from typing import TypeVar

T_Type = TypeVar("T_Type", bound=type)


def all_subclasses(base: T_Type) -> list[T_Type]:
    """Returns a list (without duplicates) of all subclasses of a given class, sorted on import-path-name.
    Ignores classes not part of the main library, e.g. subclasses part of tests.
    """
    return sorted(
        filter(
            lambda sub: str(sub).startswith("<class 'cognite.powerops"),
            set(base.__subclasses__()).union(s for c in base.__subclasses__() for s in all_subclasses(c)),
        ),
        key=str,
    )


def all_concrete_subclasses(base: T_Type) -> list[T_Type]:
    return [
        sub
        for sub in all_subclasses(base)
        if all(base is not abc.ABC for base in sub.__bases__) and not inspect.isabstract(sub)
    ]


def chunks(sequence: Sequence[T_Type], chunk_size: int) -> Iterable[Sequence[T_Type]]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(sequence), chunk_size):
        yield sequence[i : i + chunk_size]
