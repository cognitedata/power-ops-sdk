from __future__ import annotations

from pathlib import Path

from rich import print

from cognite.powerops import PowerOpsClient
from cognite.powerops.resync.v2.shop_to_assets import PowerAssetImporter
from cognite.powerops.resync.v2.config_to_fdm import ConfigImporter


def apply2(config_dir: Path, client: PowerOpsClient | None = None) -> None:
    client = client or PowerOpsClient.from_settings()

    # importer = PowerAssetImporter.from_directory(config_dir / "production")

    # assets = importer.to_power_assets()

    # client.v1.upsert(assets)

    # print(f"Upserted {len(assets)} assets")

    importer = ConfigImporter()
    things = importer.config_to_fdm()

    print(things)


    # bid_config_importer = ConfigImporter.from_directory(config_dir / "market")

    # print(bid_config_importer)


if __name__ == "__main__":
    apply2(Path("/Users/nina.odegard@cognitedata.com/Documents/GitHub/Cognite/PowerOps/power-ops-sdk/tests/data/demo"), None)