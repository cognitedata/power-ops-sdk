from __future__ import annotations

import json
import re
import string
import warnings
from functools import lru_cache
from io import StringIO
from pathlib import Path
from typing import Any
from yaml import safe_dump, safe_load
from cognite.client.utils._text import to_camel_case

# � character is used to represent unrecognizable characters in utf-8.
UNRECOGNIZABLE_CHARACTER = "�"
VALID_CHARACTERS = set(
    string.ascii_lowercase
    + string.ascii_uppercase
    + string.digits
    + UNRECOGNIZABLE_CHARACTER
    + string.punctuation
    + string.whitespace
    + "æøåÆØÅ"
)

_READ_ONLY_FIELDS = ["created_time", "last_updated_time", "uploaded_time", "data_set_id", "id", "parent_id", "root_id"]


def try_load_list(value: str | Any) -> Any | list[Any]:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return []
    return value


def try_load_dict(value: str | Any) -> Any | dict[str, Any]:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return value


def remove_read_only_fields(cdf_resource: dict[str, Any], remove_empty: bool = True) -> dict[str, Any]:
    for field in _READ_ONLY_FIELDS:
        cdf_resource.pop(field, None)
        cdf_resource.pop(to_camel_case(field), None)
    if remove_empty:
        for field in list(cdf_resource):
            if not cdf_resource[field]:
                cdf_resource.pop(field)
    return cdf_resource


def _validate(yaml_path: Path):
    if yaml_path.suffix not in {".yaml", ".yml"}:
        raise ValueError(f"File {yaml_path.name} not a valid yaml {yaml_path.suffix}")


@lru_cache
def load_yaml(yaml_path: Path, encoding="utf-8", clean_data: bool = False) -> dict:
    _validate(yaml_path)
    # The Windows Cpython implementation seems to guess if encoding is not explicitly set
    # This turns out to be a problem as it guesses wrong, which is not the case for Unix systems.
    data = Path(yaml_path).read_text(encoding=encoding)

    if clean_data and (invalid_characters := (set(data) - VALID_CHARACTERS)):
        data = re.sub(rf"[{'|'.join(invalid_characters)}]", UNRECOGNIZABLE_CHARACTER, data)
        warnings.warn(
            f"File {yaml_path.parent}/{yaml_path.name} contains invalid characters: {', '.join(invalid_characters)}"
        )
    return safe_load(StringIO(data))


# Having a single yaml dump function makes it easy to look up all places YaMLs are dumped.
# It also enables one place to set the default encoding.
def dump_yaml(data: dict, yaml_path: Path, encoding="utf-8"):
    _validate(yaml_path)
    with open(yaml_path, "w", encoding=encoding) as stream:
        safe_dump(data, stream)
