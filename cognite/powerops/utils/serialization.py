from __future__ import annotations

import contextlib
import importlib
import json
import os
import re
import string
import warnings
from collections.abc import Iterator
from pathlib import Path
from typing import Annotated, Any, ForwardRef, Union, get_args, get_origin

import tomli_w
from cognite.client.data_classes import TimeSeries
from cognite.client.utils._text import to_camel_case
from pydantic.v1.typing import evaluate_forwardref
from yaml import CSafeLoader, safe_dump

try:
    import tomllib  # type: ignore[import]
except ModuleNotFoundError:
    import tomli as tomllib  # py < 3.11


def read_toml_file(toml_file: Path | str) -> dict[str, Any]:
    """
    Read a toml file and return a dictionary.

    Args:
        toml_file: The path to the toml file.

    Returns:
        A dictionary with the toml data.
    """
    return tomllib.loads(Path(toml_file).read_text())


def dump_toml_file(toml_file: Path | str, data: dict[str, Any]) -> None:
    """
    Dump a dictionary to a toml file.

    Args:
        toml_file: The path to the toml file.
        data: The data to dump.

    """
    Path(toml_file).write_text(tomli_w.dumps(data))


@contextlib.contextmanager
def chdir(new_dir: Path) -> Iterator[None]:
    """
    Change directory to new_dir and return to the original directory when exiting the context.

    Args:
        new_dir: The new directory to change to.

    """
    current_working_dir = Path.cwd()
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
    """
    Fast loading of a yaml file.

    Args:
        yaml_path: The path to the yaml file.
        encoding: The encoding of the yaml file. Defaults to utf-8.
        clean_data: Whether to clean the data from invalid characters. Defaults to False.

    Returns:
        The data in the yaml file as a dictionary.
    """

    _validate(yaml_path)
    # The Windows Cpython implementation seems to guess if encoding is not explicitly set
    # This turns out to be a problem as it guesses wrong, which is not the case for Unix systems.
    data = Path(yaml_path).read_text(encoding=encoding)

    if clean_data and (invalid_characters := (set(data) - VALID_CHARACTERS)):
        data = re.sub(rf"[{'|'.join(invalid_characters)}]", UNRECOGNIZABLE_CHARACTER, data)
        warnings.warn(
            f"File {yaml_path.parent}/{yaml_path.name} contains invalid characters: {', '.join(invalid_characters)}",
            stacklevel=2,
        )
    return CSafeLoader(data).get_data()


def dump_yaml(yaml_path: Path, data: dict, encoding="utf-8") -> None:
    """
    Dump a dictionary to a yaml file.

    Args:
        yaml_path: The path to the yaml file.
        data: The data to dump.
        encoding: The encoding of the yaml file. Defaults to utf-8.

    """
    _validate(yaml_path)
    with yaml_path.open("w", encoding=encoding) as stream:
        safe_dump(data, stream)


def get_pydantic_annotation(field_annotation: Any, cls_obj: type[object]) -> tuple[Any, type[dict] | type[list] | None]:
    if isinstance(field_annotation, ForwardRef):
        module = vars(importlib.import_module(cls_obj.__module__))
        parent_module = vars(importlib.import_module(cls_obj.__module__.rsplit(".", maxsplit=1)[0]))
        module.update(parent_module)
        field_annotation = evaluate_forwardref(field_annotation, globals(), module)

    outer: type[dict] | type[list] | None
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
        return get_pydantic_annotation(annotation, cls_obj)
    elif origin is Annotated:
        return get_args(field_annotation)[0], None
    else:
        raise NotImplementedError(f"Cannot handle field_annotation  {field_annotation}")
    if inner := get_args(annotation):
        annotation = inner[0]
    return annotation, outer
