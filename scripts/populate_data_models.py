from cognite.powerops.cli import apply
from tests.constants import REPO_ROOT


def main():
    demo_data = REPO_ROOT / "tests" / "test_unit" / "test_bootstrap" / "data" / "demo"
    apply(
        demo_data,
        "dayahead",
        [
            "ProductionDataModel",
            "CogShopDataModel",
            "BenchmarkMarketDataModel",
            "DayAheadMarketDataModel",
            "RKOMMarketDataModel",
        ],
    )


if __name__ == "__main__":
    main()
