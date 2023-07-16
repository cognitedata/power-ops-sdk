import contextlib
from itertools import product
from pathlib import Path

import pytest

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # py < 3.11

from cognite.client.testing import monkeypatch_cognite_client

from cognite.powerops.resync._main import AVAILABLE_MODELS, apply
from tests.constants import REPO_ROOT, SENSITIVE_TESTS
from tests.test_unit.test_bootstrap.approval_test.mock_resource_create_classes import (
    MockAssetsCreate,
    MockEventsCreate,
    MockFilesUploadBytes,
    MockInstancesApply,
    MockLabelsCreate,
    MockRelationshipsCreate,
    MockSequencesCreate,
    MockTimeSeriesCreate,
)

APPROVAL_TEST = Path(__file__).resolve().parent

DATA = APPROVAL_TEST.parent / "data"
DEMO_OUT = APPROVAL_TEST / "test_apply"


def apply_test_cases():
    yield pytest.param(DATA / "demo", "Dayahead", DEMO_OUT / "demo.yml", id="Demo Case")

    # This test will be skipped if the file sensitive_tests.toml does not exist
    if not SENSITIVE_TESTS.exists():
        return

    sensitive = tomllib.loads(SENSITIVE_TESTS.read_text())
    if "tests" not in sensitive.get("Approval", {}):
        return
    for test_case in sensitive["Approval"]["tests"]:
        yield pytest.param(
            REPO_ROOT / test_case["input_dir"],
            test_case["market"],
            REPO_ROOT / test_case["compare_file_path"],
            id=test_case["name"],
        )


@pytest.mark.parametrize(
    "input_dir, market, compare_file_path, model_name",
    list(
        pytest.param(*case.values, model_name, id=f"{case.id} {model_name}")
        for case, model_name in product(apply_test_cases(), AVAILABLE_MODELS)
    ),
)
def test_apply(
    input_dir: Path, market: str, compare_file_path: Path, model_name: str, data_regression, setting_environmental_vars
):
    # Arrange
    mock_resources = {
        "assets.create": MockAssetsCreate(),
        "sequences.create": MockSequencesCreate(),
        "relationships.create": MockRelationshipsCreate(),
        "time_series.create": MockTimeSeriesCreate(),
        "labels.create": MockLabelsCreate(),
        "events.create": MockEventsCreate(),
        "data_modeling.instances.apply": MockInstancesApply(),
        "files.upload_bytes": MockFilesUploadBytes(),
    }

    with monkeypatch_cognite_client() as client:
        client.config.project = "cdf-project"
        for resource_name, mock_resource in mock_resources.items():
            parts = resource_name.split(".")
            api = client
            for resource in parts[:-1]:
                api = getattr(api, resource)
            setattr(api, parts[-1], mock_resource)

        # Act
        model = apply(path=DATA / "demo", market="Dayahead", model_names=model_name, auto_yes=True)

    # Assert
    data_regression.check(
        model.summary(), fullpath=compare_file_path.parent / f"{compare_file_path.stem}_{model_name}_summary.yml"
    )

    dump = {
        ".".join(resource_type.split(".")[:-1]): mock_resource.serialize()
        for resource_type, mock_resource in mock_resources.items()
    }
    # for all the resources, sort the list of dicts by "external_id" in lowercase
    for resource in dump.values():
        with contextlib.suppress(KeyError):
            resource.sort(key=lambda x: x["external_id"].lower())

    # Assert
    data_regression.check(dump, fullpath=compare_file_path.parent / f"{compare_file_path.stem}_{model_name}.yml")
