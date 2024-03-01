from __future__ import annotations

from pathlib import Path

from rich import print

from cognite.powerops import PowerOpsClient
from cognite.powerops.resync.v2.shop_to_assets import PowerAssetImporter


def apply2(config_dir: Path, client: PowerOpsClient | None = None) -> None:
    client = client or PowerOpsClient.from_settings()

    importer = PowerAssetImporter.from_directory(config_dir / "production")

    assets = importer.to_power_assets()

    client.v1.upsert(assets)

    print(f"Upserted {len(assets)} assets")
