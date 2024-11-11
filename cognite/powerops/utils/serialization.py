from __future__ import annotations

import re
import string
import warnings
from pathlib import Path
from typing import Literal, overload

from yaml import CSafeLoader

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


def _validate(yaml_path: Path) -> None:
    if yaml_path.suffix not in {".yaml", ".yml"}:
        raise ValueError(f"File {yaml_path.name} not a valid yaml {yaml_path.suffix}")


@overload
def load_yaml(
    yaml_path: Path, expected_return_type: Literal["dict"] = "dict", encoding: str = "utf-8", clean_data: bool = False
) -> dict: ...


@overload
def load_yaml(
    yaml_path: Path, expected_return_type: Literal["list"], encoding: str = "utf-8", clean_data: bool = False
) -> list: ...


@overload
def load_yaml(
    yaml_path: Path, expected_return_type: Literal["any"], encoding: str = "utf-8", clean_data: bool = False
) -> list | dict: ...


def load_yaml(
    yaml_path: Path,
    expected_return_type: Literal["dict", "list", "any"] = "any",
    encoding: str = "utf-8",
    clean_data: bool = False,
) -> dict | list:
    """
    Fast loading of a yaml file.

    Args:
        yaml_path: The path to the yaml file.
        expected_return_type: The expected return type. The function will raise an error
                              if the file does not return the expected type. Defaults to any.
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
    output = CSafeLoader(data).get_data()
    if expected_return_type == "dict" and not isinstance(output, dict):
        if not output:
            warnings.warn(
                f"File {yaml_path.parent}/{yaml_path.name} contains no data",
                stacklevel=2,
            )
            return {}
        raise ValueError(f"Expected a dictionary, got {type(output)}")
    if expected_return_type == "list" and not isinstance(output, list):
        if not output:
            warnings.warn(
                f"File {yaml_path.parent}/{yaml_path.name} contains no data",
                stacklevel=2,
            )
            return []
        raise ValueError(f"Expected a list, got {type(output)}")
    return output
