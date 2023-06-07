from __future__ import annotations

from collections import UserDict, UserList
from typing import Any, Generic, TypeVar, Union

_undefined = object()


DotTypeT = TypeVar("DotTypeT", bound=Union[dict, list])


class DotGet(Generic[DotTypeT]):
    data: DotTypeT

    def __repr__(self) -> str:
        return repr(self.data)

    def _wrap(self, value: Any) -> Any:
        """
        When returning a value, if it is a dict or a list, wrap it in a new DotGet.
        Otherwise just return it as-is.
        Note: self.data should always remain "pure" dict or list.
        """
        if isinstance(value, dict):
            dot_item = DotDict()
            dot_item.data = value
            return dot_item
        elif isinstance(value, list):
            dot_item = DotList()
            dot_item.data = value
            return dot_item
        return value

    def __getitem__(self, item: str) -> Any:
        """
        Itemgetter with support for "." as separator on nested dict/list structures.
        """
        try:
            value = super().__getitem__(item)
        except (ValueError, TypeError, KeyError, IndexError):
            scope = self.data
            items = str(item).split(".")
            while items:
                part = items.pop(0)
                next_scope = _undefined
                for _type in self._possible_types(part):
                    try:
                        next_scope = scope[_type(part)]
                    except (ValueError, TypeError, KeyError, IndexError):
                        pass
                    else:
                        break
                if next_scope is _undefined:
                    if isinstance(scope, list):
                        raise IndexError(".".join([part, *items]))
                    else:
                        raise KeyError(".".join([part, *items]))

                scope = next_scope
            else:
                value = scope
        return self._wrap(value)

    def __setitem__(self, key, value) -> Any:
        """
        Itemsetter with support for "." as a separator for nested dict/list structures.

        Caution: this creates ambiguity: foo["a.b.c"] = 123 could mean foo["a"]["b"]["c"] = 123, but
        it could also mean to literally set value 123 for key "a.b.c". Or any combination with "a.b" and "b.c".

        This class will always split the keys at ".". If any of the parent keys are missing a KeyError will be
        raised. It is up to the user to ensure the structure exists before assigning values to it.

        To assign keys that actually contain a dot, use foo.data["a.b.c"] = 123.
        """
        if isinstance(value, DotGet):
            value = value.data
        key = str(key)
        if "." in key:
            subpath, _, leaf_key = str(key).rpartition(".")
            subitem = self[subpath]
        else:
            subitem = self
            leaf_key = key
        for _type in self._possible_types(leaf_key):
            try:
                _ = subitem.data[_type(leaf_key)]
            except (TypeError, ValueError, IndexError, KeyError):  # TODO DRY this!
                pass
            else:
                subitem.data[_type(leaf_key)] = value
                break
        else:
            subitem.data[leaf_key] = value
        return value

    def __eq__(self, other):
        return self.data == other

    @staticmethod
    def _possible_types(part):
        """
        Helper for dealing with type ambiguity in keys / indexes,
        e.g: foo["a.1"] could mean foo["a"][1] or foo["a"]["1"].
        """
        first_type = type(part)
        all_types = {str, int, float}
        return [first_type, *(all_types - {first_type})]


class DotList(DotGet[list], UserList, list):
    def list(self) -> list:
        return self.data


class DotDict(DotGet[dict], UserDict, dict):
    def dict(self) -> dict:
        return self.data
