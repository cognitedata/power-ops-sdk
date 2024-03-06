import arrow
import pandas as pd
import pytest

from cognite.powerops import PowerOpsClient
from cognite.powerops.client.data_classes import SHOPRun, SHOPRunList, SHOPRunStatus


@pytest.fixture(scope="session")
def shop_run_list(powerops_client) -> SHOPRunList:
    return powerops_client.shop.list(start_after=arrow.get("2024-02-01T00:00:00Z"), limit=5)


@pytest.fixture(scope="session")
def shop_run(shop_run_list: SHOPRunList) -> SHOPRun:
    return shop_run_list[0]


@pytest.fixture(scope="session")
def shop_run_success(powerops_client: PowerOpsClient) -> SHOPRun:
    return powerops_client.shop.retrieve("SHOP_RUN_2023-09-12T16:19:14.342892Z_110802")


@pytest.mark.cdf
def test_list_shop_runs(powerops_client: PowerOpsClient) -> None:
    runs = powerops_client.shop.list(limit=5)

    assert len(runs) == 5
    assert isinstance(runs.to_pandas(), pd.DataFrame)


@pytest.mark.cdf
def test_retrieve_shop_run(powerops_client: PowerOpsClient, shop_run_list: SHOPRunList) -> None:
    run = powerops_client.shop.retrieve(shop_run_list[0].external_id)

    assert isinstance(run, SHOPRun)
    assert run == shop_run_list[0]


@pytest.mark.cdf
def test_get_case_file(shop_run: SHOPRun) -> None:
    case_file = shop_run.get_case_file()

    assert isinstance(case_file, str)


@pytest.mark.cdf
def test_get_shop_files(shop_run: SHOPRun) -> None:
    shop_files = list(shop_run.get_shop_files())

    assert isinstance(shop_files, list)
    assert shop_files, "No shop files found"
    for shop_file in shop_files:
        assert isinstance(shop_file, str)


@pytest.mark.cdf
def test_get_pre_run_file(shop_run: SHOPRun) -> None:
    pre_run_file = shop_run.get_pre_run_file()

    assert isinstance(pre_run_file, str)


@pytest.mark.cdf
def test_check_status(shop_run: SHOPRun) -> None:
    status = shop_run.check_status()

    assert isinstance(status, SHOPRunStatus)


@pytest.mark.cdf
def test_get_log_files(shop_run_success: SHOPRun) -> None:
    result_files = list(shop_run_success.get_log_files())

    assert isinstance(result_files, list)
    assert result_files, "No result files found"
    for result_file in result_files:
        assert isinstance(result_file, tuple)
        assert isinstance(result_file[0], str)
        assert isinstance(result_file[1], str)


@pytest.mark.cdf
def test_case_from_event(powerops_client: PowerOpsClient) -> None:
    shop_run = powerops_client.shop.retrieve("POWEROPS_SHOP_RUN_2023-10-26T09:56:27.964670Z_efc805")
    case = shop_run.to_case()
    assert all(key in case.data for key in ["commands", "time", "model", "connections"])
