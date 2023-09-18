import os
from pathlib import Path

import pytest
from coverage.tomlconfig import tomllib
from loguru import logger
from yaml import safe_load

from cognite.powerops import resync
from cognite.powerops.client.powerops_client import PowerOpsClient
from cognite.powerops.utils.serialization import chdir
from tests.constants import REPO_ROOT, SENSITIVE_TESTS, ReSync

THIS_FOLDER = Path(__file__).resolve().parent
PLAN = THIS_FOLDER / "plan"
PLAN.mkdir(exist_ok=True)


def plan_test_cases():
    for model in ["ProductionModel", "MarketModel", "CogShop1Asset"]:
        yield pytest.param(ReSync.demo, "Dayahead", model, PLAN, "settings.toml;.secrets.toml", id=f"Demo Data {model}")

    # This test will be skipped if the file sensitive_tests.toml does not exist
    if not SENSITIVE_TESTS.exists():
        return

    sensitive = tomllib.loads(SENSITIVE_TESTS.read_text())
    if "tests" not in sensitive.get("Plan", {}):
        return
    for test_case in sensitive["Plan"]["tests"]:
        yield pytest.param(
            REPO_ROOT / Path(test_case["data_path"]),
            test_case["market"],
            test_case["model_name"],
            REPO_ROOT / Path(test_case["dump_folder"]),
            test_case["config_file"],
            id=f"{test_case['data_path']} {test_case['model_name']}",
        )

@pytest.mark.skip("Temporaryly skip")
@pytest.mark.parametrize("data_path, market, model_name, dump_folder, config_file", list(plan_test_cases()))
def test_plan(
    data_path: Path, market: str, model_name: str, dump_folder: Path, config_file: str, data_regression
) -> None:
    # Arrange
    with chdir(REPO_ROOT):
        os.environ["SETTINGS_FILES"] = config_file
        powerops_client = PowerOpsClient.from_settings()

        # Act
        resync.plan(
            data_path,
            market,
            echo=logger.info,
            model_names=[model_name],
            dump_folder=dump_folder,
            client=powerops_client,
        )

    # Assert
    data = safe_load((dump_folder / f"{model_name}_local.yaml").read_text())
    data_regression.check(data, fullpath=dump_folder / f"{model_name}_local_check.yml")
