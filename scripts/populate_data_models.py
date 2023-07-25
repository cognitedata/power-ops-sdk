from cognite.powerops.cli import apply
from tests.constants import REPO_ROOT


def main():
    demo_data = REPO_ROOT / "tests" / "test_unit" / "test_bootstrap" / "data" / "demo"
    models = [
        "ProductionDataModel",
        "CogShopDataModel",
        "BenchmarkMarketDataModel",
        "DayAheadMarketDataModel",
        "RKOMMarketDataModel",
    ]
    for model in models:
        apply(
            demo_data,
            "dayahead",
            [model],
            auto_yes=True,
        )


if __name__ == "__main__":
    main()
