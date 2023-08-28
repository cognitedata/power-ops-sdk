from cognite.powerops.cli import apply
from tests.constants import ReSync, REPO_ROOT
from tests.utils import chdir


def main():
    with chdir(REPO_ROOT):
        apply(ReSync.demo, "DayAhead", ["ProductionModel"])


if __name__ == "__main__":
    main()
