from __future__ import annotations

from cognite.powerops.config import BootstrapConfig

# TODO we probably don't need this, since configuration will be entirely local


class ConfigurationClient:
    def retrieve(self, external_id: str) -> BootstrapConfig:
        ...

    def update(self, configuration: BootstrapConfig) -> BootstrapConfig:
        ...

    def delete(self, configuration: BootstrapConfig) -> None:
        ...

    def copy(self, configuration: BootstrapConfig, external_id: str) -> BootstrapConfig:
        """Create a copy of an existing configuration with a new external_id"""
