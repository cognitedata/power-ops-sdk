import getpass
import logging
import os
from pathlib import Path
from typing import Any, Literal, Optional

import pydantic

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # py < 3.11

from pydantic import BaseModel, BaseSettings, validator

__all__ = ["Settings", "CogniteSettings", "PoweropsRunSettings"]


logger = logging.getLogger(__name__)


class CogniteSettings(BaseModel):
    login_flow: Literal["client_credentials", "interactive"]
    project: str
    cdf_cluster: str
    tenant_id: str
    client_id: str
    client_secret: Optional[str]

    client_name: str = ""
    authority_host_uri: str = "https://login.microsoftonline.com"

    @property
    def base_url(self) -> str:
        return f"https://{self.cdf_cluster}.cognitedata.com/"

    @property
    def scopes(self) -> list[str]:
        return [f"{self.base_url}.default"]

    @property
    def authority_uri(self) -> str:
        return f"{self.authority_host_uri}/{self.tenant_id}"

    @property
    def token_url(self) -> str:
        return f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"

    @validator("client_name", always=True)
    def replace_none_with_user(cls, value):
        return getpass.getuser() if value is None else value


class PoweropsRunSettings(pydantic.BaseModel):
    read_dataset: Optional[str]
    write_dataset: Optional[str]
    cogshop_version: Optional[str]


class Settings(pydantic.BaseSettings):
    cognite: CogniteSettings = {}
    powerops: PoweropsRunSettings = {}

    class Config:
        env_prefix = "SETTINGS__"
        env_nested_delimiter = "__"

        @classmethod
        def customise_sources(cls, init_settings, env_settings, file_secret_settings):
            """Add `file_settings` to sources (loads settings.toml and .secrets.toml)."""
            return init_settings, env_settings, _file_settings, file_secret_settings


def _file_settings(_settings: BaseSettings) -> dict[str, Any]:
    settings_files = filter(None, os.environ.get("SETTINGS_FILES", "settings.toml;.secrets.toml").split(";"))
    collected_data = {}
    for file_path in settings_files:
        try:
            data = tomllib.loads(Path(file_path).read_text())
        except FileNotFoundError:
            pass
        else:
            logger.debug(f"Loaded settings from '{file_path}'.")
            # Merge dictionaries, assume one level of nesting.
            for key in collected_data.keys() | data.keys():
                if key in collected_data and key in data:
                    collected_data[key] = collected_data[key] | data[key]
                elif key in data:
                    collected_data[key] = data[key]
    return collected_data
