from typing import Optional

import pydantic

from cognite.powerops.settings.loader import file_settings

__all__ = ["Settings"]


class CogniteSettings(pydantic.BaseModel):
    project: Optional[str]
    cdf_cluster: Optional[str]
    tenant_id: Optional[str]
    client_id: Optional[str]
    client_secret: Optional[str]
    space: Optional[str]
    data_model: Optional[str]
    schema_version: Optional[str]


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
            return init_settings, env_settings, file_settings, file_secret_settings
