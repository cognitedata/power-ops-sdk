from __future__ import annotations

from pathlib import Path

from rich import print

from cognite.powerops import PowerOpsClient
from cognite.powerops.client._generated.v1.data_classes import (
    GeneratorWrite,
    PlantInformationWrite,
)
from cognite.powerops.resync.v2.config_to_fdm import ConfigImporter


def apply2(config_dir: Path, client: PowerOpsClient | None = None) -> None:
    client = client or PowerOpsClient.from_settings()

    expected_types = [
        # PriceAreaInformationWrite,
        # MarketConfigurationWrite,
        # ShopCommandsWrite,
        # ShopAttributeMappingWrite,
        # ShopModelWrite,
        # ShopScenarioWrite,
        # ShopScenarioSetWrite,
        # ShopBasedPartialBidConfigurationWrite,
        # BidConfigurationDayAheadWrite,
        # WaterValueBasedPartialBidConfigurationWrite,
        GeneratorWrite,
        PlantInformationWrite,
    ]
    day_ahead_importer = ConfigImporter.from_directory(config_dir / "v1", expected_types)
    day_ahead_config = day_ahead_importer.config_to_fdm()

    print(day_ahead_config)

    print(f"Upserted {len(day_ahead_config)} bid configurations")


if __name__ == "__main__":
    apply2(Path("/Users/nina.odegard@cognitedata.com/Documents/GitHub/Cognite/PowerOps/power-ops-sdk/tests/data/demo"))
