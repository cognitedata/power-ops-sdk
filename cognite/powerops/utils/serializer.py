from __future__ import annotations

import re
import warnings
from functools import lru_cache
from io import StringIO
from pathlib import Path

from yaml import safe_dump, safe_load

from cognite.powerops.utils.constants import UNRECOGNIZABLE_CHARACTER, VALID_CHARACTERS


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
