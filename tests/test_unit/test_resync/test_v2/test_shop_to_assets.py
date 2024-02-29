from collections import Counter

from cognite.powerops.resync.v2.shop_to_assets import PowerAssetImporter
from tests.constants import ReSync


class TestShopToAssets:
    def test_demo_data_to_assets(self) -> None:
        importer = PowerAssetImporter.from_directory(ReSync.production)

        assets = importer.to_power_assets()

        counts = Counter([type(asset).__name__ for asset in assets])

        assert counts["GeneratorWrite"] == 12
        assert counts["PlantWrite"] == 9
        assert counts["ReservoirWrite"] == 16
        assert counts["WatercourseWrite"] == 1
        assert counts["PriceAreaWrite"] == 1
