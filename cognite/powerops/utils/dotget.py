from __future__ import annotations

from collections import UserDict, UserList
from contextlib import suppress
from typing import Any

_undefined = object()


class DotGet:
    def _get(self, item):
        value = _undefined
        try:
            value = super().__getitem__(item)
        except (TypeError, KeyError, IndexError):
            for key_type in (int, float, bool):
                with suppress(TypeError):
                    try:
                        value = super().__getitem__(key_type(item))
                    except (TypeError, KeyError, IndexError):
                        pass
                    else:
                        break
            if value is _undefined:
                raise KeyError()
        return self._wrap(value)

    def __repr__(self):
        return repr(self.data)
        # return f"<{type(self).__name__} {repr(self.data)}>"

    def _wrap(self, value):
        if isinstance(value, dict):
            dot_item = DotDict()
            dot_item.data = value
            return dot_item
        elif isinstance(value, list):
            dot_item = DotList()
            dot_item.data = value
            return dot_item
        return value

    @staticmethod
    def _possible_types(part):
        first_type = type(part)
        all_types = {str, int, float}
        return [first_type, *(all_types - {first_type})]

    def __getitem__(self, item: str) -> Any:
        """Just a dict but with support for "." as separator on nested dict structures."""
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


class DotList(DotGet, UserList):
    pass


class DotDict(DotGet, UserDict):
    pass
