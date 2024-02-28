from __future__ import annotations

from pathlib import Path
from typing import Any

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelWrite,
)
from cognite.powerops.utils.serialization import load_yaml


class PowerAssetImporter:
    def __init__(self, shop_models: list[dict[str, Any]]):
        self.shop_models = shop_models

    @classmethod
    def from_directory(cls, directory: Path, file_name: str = "model_raw") -> PowerAssetImporter:
        shop_model_files = list(directory.glob(f"**/{file_name}.yaml"))
        shop_models = [load_yaml(file) for file in shop_model_files]
        return cls(shop_models)

    def to_power_assets(self) -> list[DomainModelWrite]:
        return []
