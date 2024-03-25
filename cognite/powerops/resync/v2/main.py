from __future__ import annotations

from pathlib import Path

from rich import print

from cognite.powerops import PowerOpsClient
from cognite.powerops.client._generated.v1.data_classes import (
    MarketConfigurationWrite,
    PriceAreaWrite,
    PriceScenarioWrite,
)
from cognite.powerops.resync.v2.config_to_fdm import ConfigImporter
from cognite.powerops.resync.v2.shop_to_assets import PowerAssetImporter


def apply2(config_dir: Path, client: PowerOpsClient | None = None) -> None:
    client = client or PowerOpsClient.from_settings()

    importer = PowerAssetImporter.from_directory(config_dir / "production")

    assets = importer.to_power_assets()

    client.v1.upsert(assets)

    print(f"Upserted {len(assets)} assets")

    expected_types = [PriceScenarioWrite, MarketConfigurationWrite, PriceAreaWrite]
    day_ahead_importer = ConfigImporter.from_directory(config_dir / "market" / "v2", expected_types)
    day_ahead_config = day_ahead_importer.config_to_fdm()

    client.v1.upsert(day_ahead_config)

    print(f"Upserted {len(day_ahead_config)} bid configurations")
