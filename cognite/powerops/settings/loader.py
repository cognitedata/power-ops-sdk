import logging
import os
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # py < 3.11

from pydantic import BaseSettings

from cognite.powerops.utils.common import merge

logger = logging.getLogger(__name__)


def file_settings(_settings: BaseSettings) -> dict[str, Any]:
    settings_files = filter(None, os.environ.get("SETTINGS_FILES", "settings.toml;.secrets.toml").split(";"))
    collected_data = {}
    for file_path in settings_files:
        try:
            data = _load_file(file_path)
        except FileNotFoundError:
            pass
        else:
            logger.debug(f"Loaded settings from '{file_path}'.")
            collected_data = merge(collected_data, data)
    return collected_data


def _load_file(file_path: str) -> dict:
    with open(file_path, "rb") as toml_file:
        return tomllib.load(toml_file)
