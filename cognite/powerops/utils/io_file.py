from __future__ import annotations
from pathlib import Path
from typing import Any
import tomli_w

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # py < 3.11


def read_toml_file(toml_file: Path | str) -> dict[str, Any]:
    return tomllib.loads(Path(toml_file).read_text())


def write_toml_file(toml_file: Path | str, data: dict[str, Any]) -> None:
    Path(toml_file).write_text(tomli_w.dumps(data))
