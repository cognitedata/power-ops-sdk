import pandas as pd
import pytest

from cognite.powerops import PowerOpsClient
from cognite.powerops.client.data_classes import SHOPRun, SHOPRunList


@pytest.fixture(scope="session")
def shop_run_list(powerops_client) -> SHOPRunList:
    return powerops_client.shop.list(limit=5)


def test_list_shop_runs(powerops_client: PowerOpsClient) -> None:
    runs = powerops_client.shop.list(limit=5)

    assert len(runs) == 5
    assert isinstance(runs.to_pandas(), pd.DataFrame)


def test_retrieve_shop_run(powerops_client: PowerOpsClient, shop_run_list: SHOPRunList) -> None:
    run = powerops_client.shop.retrieve(shop_run_list[0].external_id)

    assert isinstance(run, SHOPRun)
    assert run == shop_run_list[0]
