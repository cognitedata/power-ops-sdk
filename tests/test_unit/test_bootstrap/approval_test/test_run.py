import contextlib
from pathlib import Path

from cognite.client.testing import monkeypatch_cognite_client

from cognite.powerops.main import apply
from tests.test_unit.test_bootstrap.approval_test.mock_resource_create_classes import (
    MockAssetsCreate,
    MockEventsCreate,
    MockLabelsCreate,
    MockRelationshipsCreate,
    MockSequencesCreate,
    MockTimeSeriesCreate,
)

DATA = Path(__file__).resolve().parent.parent / "data"


def test_run(data_regression):
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
    data_regression.check(dump, basename="demo")
