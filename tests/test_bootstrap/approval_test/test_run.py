import contextlib

import pytest

from cognite.client.testing import monkeypatch_cognite_client

import bootstrap

from bootstrap.run_demo import run_demo
from bootstrap.run_heco import run_heco
from bootstrap.run_lyse import run_lyse
from tests.test_bootstrap.approval_test.mock_resource_create_classes import (
    MockAssetsCreate,
    MockEventsCreate,
    MockLabelsCreate,
    MockRelationshipsCreate,
    MockSequencesCreate,
    MockTimeSeriesCreate,
)


RUN_FUNCTION_BY_CUSTOMER = {
    "demo": run_demo,
    "lyse": run_lyse,
    "heco": run_heco,
}


def _run_skip_dm(customer):
    original = getattr(bootstrap, f"run_{customer}")._run

    def _run_patched(config, case, time_series_mappings, market="Dayahead"):
        config.constants.skip_dm = True
        return original(config, case, time_series_mappings, market)

    return _run_patched


@pytest.mark.parametrize("customer", RUN_FUNCTION_BY_CUSTOMER)
def test_run(customer, data_regression, mocker):
    mock_resources = {
        "assets": MockAssetsCreate(),
        "sequences": MockSequencesCreate(),
        "relationships": MockRelationshipsCreate(),
        "time_series": MockTimeSeriesCreate(),
        "labels": MockLabelsCreate(),
        "events": MockEventsCreate(),
    }

    with monkeypatch_cognite_client() as client:
        mocker.patch(f"bootstrap.run_{customer}._run", _run_skip_dm(customer))
        for resource_name, mock_resource in mock_resources.items():
            api = getattr(client, resource_name)
            api.create = mock_resource
        run = RUN_FUNCTION_BY_CUSTOMER[customer]
        run()

    dump = {resource_type: mock_resource.serialize() for resource_type, mock_resource in mock_resources.items()}

    # for all the resources, sort the list of dicts by "external_id" in lowercase
    for resource in dump.values():
        with contextlib.suppress(KeyError):
            resource.sort(key=lambda x: x["external_id"].lower())
    data_regression.check(dump, basename=customer)
