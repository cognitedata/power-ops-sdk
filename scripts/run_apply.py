from cognite.powerops.cli import apply
from tests.constants import ReSync, REPO_ROOT
from cognite.powerops.utils.serialization import chdir


def main():
    with chdir(REPO_ROOT):
        apply(
            ReSync.demo,
            "Dayahead",
            ["ProductionModel", "MarketModel", "CogShop1Asset"],
            auto_yes=False,
            format="markdown",
            verbose=False,
        )


if __name__ == "__main__":
    main()
