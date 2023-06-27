import logging
import os
from typing import Any

import tomli
from mergedeep import merge
from pydantic import BaseSettings

logger = logging.getLogger(__name__)

SETTINGS_FILES = os.environ.get("SETTINGS_FILES", "settings.toml;.secrets.toml").split(";")


def file_settings(_settings: BaseSettings) -> dict[str, Any]:
    collected_data = {}
    for file_path in SETTINGS_FILES:
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
        return tomli.load(toml_file)
