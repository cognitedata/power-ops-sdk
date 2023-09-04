from __future__ import annotations
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # py < 3.11


def read_toml_file(toml_file: Path | str) -> dict[str, Any]:
    return tomllib.loads(Path(toml_file).read_text())
