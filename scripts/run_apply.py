from cognite.powerops.cli import apply
from tests.constants import ReSync, REPO_ROOT
from cognite.powerops.utils.serialization import chdir


def main():
    with chdir(REPO_ROOT):
        apply(ReSync.demo, "Dayahead", ["ProductionModelDM"], auto_yes=True, format="markdown", verbose=True)


if __name__ == "__main__":
    main()
