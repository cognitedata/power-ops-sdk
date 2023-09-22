import getpass
import logging
import os
from typing import Any, Literal, Optional

import pydantic
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

__all__ = ["Settings", "CogniteSettings", "PoweropsRunSettings"]

from cognite.powerops.utils.serialization import read_toml_file

logger = logging.getLogger(__name__)


class CogniteSettings(BaseModel):
    login_flow: Literal["client_credentials", "interactive"]
    project: str
    cdf_cluster: str
    tenant_id: str
    client_id: str
    client_secret: Optional[str] = None

    client_name: str = Field(default_factory=lambda: getpass.getuser())
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


class PoweropsRunSettings(pydantic.BaseModel):
    read_dataset: Optional[str] = None
    write_dataset: Optional[str] = None
    monitor_dataset: Optional[str] = None
    cogshop_version: Optional[str] = None

    @field_validator("cogshop_version", mode="before")
    def number_to_str(cls, v):
        return str(v) if isinstance(v, (int, float)) else v


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SETTINGS__", env_nested_delimiter="__")
    cognite: CogniteSettings = Field(default_factory=dict)
    powerops: PoweropsRunSettings = Field(default_factory=dict)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Add `file_settings` to sources (loads settings.toml and .secrets.toml)."""
        return init_settings, env_settings, _file_settings, file_secret_settings  # type: ignore[return-value]


def _file_settings() -> dict[str, Any]:
    settings_files = filter(None, os.environ.get("SETTINGS_FILES", "settings.toml;.secrets.toml").split(";"))
    collected_data: dict[str, Any] = {}
    for file_path in settings_files:
        try:
            data = read_toml_file(file_path)
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
