import contextlib
from pathlib import Path

import pytest
import tomli
from cognite.client.testing import monkeypatch_cognite_client

from cognite.powerops.main import apply
from tests.constants import REPO_ROOT, SENSITIVE_TESTS
from tests.test_unit.test_bootstrap.approval_test.mock_resource_create_classes import (
    MockAssetsCreate,
    MockEventsCreate,
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

    sensitive = tomli.loads(SENSITIVE_TESTS.read_text())
    if "tests" not in sensitive.get("Approval", {}):
        return
    for test_case in sensitive["Approval"]["tests"]:
        yield pytest.param(
            REPO_ROOT / test_case["input_dir"],
            test_case["market"],
            REPO_ROOT / test_case["compare_file_path"],
            id=test_case["name"],
        )


@pytest.mark.parametrize("input_dir, market, compare_file_path", list(apply_test_cases()))
def test_apply(input_dir: Path, market: str, compare_file_path: Path, data_regression):
    mock_resources = {
        "assets": MockAssetsCreate(),
        "sequences": MockSequencesCreate(),
        "relationships": MockRelationshipsCreate(),
        "time_series": MockTimeSeriesCreate(),
        "labels": MockLabelsCreate(),
        "events": MockEventsCreate(),
    }

    with monkeypatch_cognite_client() as client:
        for resource_name, mock_resource in mock_resources.items():
            api = getattr(client, resource_name)
            api.create = mock_resource

        apply(path=DATA / "demo", market="Dayahead")

    dump = {resource_type: mock_resource.serialize() for resource_type, mock_resource in mock_resources.items()}

    # for all the resources, sort the list of dicts by "external_id" in lowercase
    for resource in dump.values():
        with contextlib.suppress(KeyError):
            resource.sort(key=lambda x: x["external_id"].lower())
    data_regression.check(dump, fullpath=compare_file_path)
