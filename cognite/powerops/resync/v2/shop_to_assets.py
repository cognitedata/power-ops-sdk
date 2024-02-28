from __future__ import annotations

from typing import Any

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelWrite,
)


class PowerAssetImporter:
    def __init__(self, shop_models: list[dict[str, Any]]):
        self.shop_models = shop_models

    def to_power_assets(self) -> list[DomainModelWrite]:
        raise NotImplementedError()
