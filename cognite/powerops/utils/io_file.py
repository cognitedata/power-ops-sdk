from __future__ import annotations

import contextlib
import os
from pathlib import Path
from typing import Any
import tomli_w

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
