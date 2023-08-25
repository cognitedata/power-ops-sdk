from pathlib import Path
import os
import pytest
from coverage.tomlconfig import tomllib

from cognite.powerops import resync

from loguru import logger

from yaml import safe_load
from tests.constants import SENSITIVE_TESTS, REPO_ROOT
from tests.utils import chdir
from cognite.powerops.clients.powerops_client import get_powerops_client


def plan_test_cases():
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


@pytest.mark.parametrize("data_path, market, model_name, dump_folder, config_file", list(plan_test_cases()))
def test_plan(
    data_path: Path, market: str, model_name: str, dump_folder: Path, config_file: str, data_regression
) -> None:
    # Arrange
    with chdir(REPO_ROOT):
        os.environ["SETTINGS_FILES"] = config_file
        powerops_client = get_powerops_client()

    # Act
    resync.plan(
        data_path, market, echo=logger.info, model_names=[model_name], dump_folder=dump_folder, client=powerops_client
    )

    # Assert
    data = safe_load((dump_folder / f"{model_name}_local.yaml").read_text())
    data_regression.check(data, fullpath=dump_folder / f"{model_name}_local_check.yml")