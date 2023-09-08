from __future__ import annotations

import contextlib
import importlib
import json
import os
import re
import string
import warnings
from pathlib import Path
from typing import Any, Type, ForwardRef, get_origin, get_args, Union
import tomli_w
from cognite.client.data_classes import TimeSeries
from cognite.client.utils._text import to_camel_case
from pydantic.v1.typing import evaluate_forwardref
from yaml import CSafeLoader, safe_dump

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # py < 3.11


def read_toml_file(toml_file: Path | str) -> dict[str, Any]:
    return tomllib.loads(Path(toml_file).read_text())


def dump_toml_file(toml_file: Path | str, data: dict[str, Any]) -> None:
    Path(toml_file).write_text(tomli_w.dumps(data))


@contextlib.contextmanager
def chdir(new_dir: Path) -> None:
    """
    Change directory to new_dir and return to the original directory when exiting the context.

    Args:
        new_dir: The new directory to change to.

    """
    current_working_dir = os.getcwd()
    os.chdir(new_dir)

    try:
        yield

    finally:
        os.chdir(current_working_dir)


# � character is used to represent unrecognizable characters in utf-8.


# Having a single yaml dump function makes it easy to look up all places YaMLs are dumped.
# It also enables one place to set the default encoding.

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
_READ_ONLY_FIELDS = [
    "created_time",
    "last_updated_time",
    "uploaded_time",
    "data_set_id",
    "id",
    "parent_id",
    "root_id",
    "uploaded",
]


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


def parse_time_series(value: str | Any) -> Any | TimeSeries:
    if isinstance(value, TimeSeries) or value is None:
        return value
    elif value == {}:
        return None
    elif isinstance(value, dict):
        return TimeSeries._load(value)
    raise NotImplementedError()


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
    return CSafeLoader(data).get_data()


def dump_yaml(data: dict, yaml_path: Path, encoding="utf-8"):
    _validate(yaml_path)
    with open(yaml_path, "w", encoding=encoding) as stream:
        safe_dump(data, stream)


def get_pydantic_annotation(field_annotation: Any, cls_obj: Type[object]) -> tuple[Any, Type[dict] | Type[list] | None]:
    if isinstance(field_annotation, ForwardRef):
        module = vars(importlib.import_module(cls_obj.__module__))
        parent_module = vars(importlib.import_module(cls_obj.__module__.rsplit(".", maxsplit=1)[0]))
        module.update(parent_module)
        field_annotation = evaluate_forwardref(field_annotation, globals(), module)

    outer: Type[dict] | Type[list] | None
    if not (origin := get_origin(field_annotation)):
        return field_annotation, None
    if origin is list:
        annotation, *_ = get_args(field_annotation)
        outer = list
    elif origin is dict:
        _, annotation = get_args(field_annotation)
        outer = dict
    elif origin is Union:
        annotation, *_ = get_args(field_annotation)
        outer = None
    else:
        raise NotImplementedError(f"Cannot handle field_annotation  {field_annotation}")
    return annotation, outer