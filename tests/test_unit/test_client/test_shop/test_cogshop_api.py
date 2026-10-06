import datetime
from unittest import mock

import pytest
import requests

from cognite.powerops.client._generated.data_classes import ShopCaseWrite, ShopModelWrite, ShopScenarioWrite
from cognite.powerops.client.shop.cogshop_api import CogShopAPI, CogShopStatus


@pytest.fixture
def mock_cdf():
    cdf = mock.Mock()
    cdf.config.project = "power-ops-staging"
    cdf.config.base_url = "https://api.cognitedata.com"
    return cdf


@pytest.fixture
def mock_po():
    po = mock.Mock()
    po.shop_based_day_ahead_bid_process.shop_scenario.retrieve.return_value = mock.Mock(external_id="scenario_ext_id")
    return po


@pytest.fixture
def cogshop_api(mock_cdf, mock_po):
    return CogShopAPI(mock_cdf, mock_po)


class TestCogShopAPI:
    def test_init_default(self, mock_cdf, mock_po):
        api = CogShopAPI(mock_cdf, mock_po)
        assert api._cdf == mock_cdf
        assert api._po == mock_po
        assert api.cog_shop_service is None
        url = api._shop_url_cshaas()
        assert url == "https://power-ops-api.staging.api.cognite.ai/power-ops-staging/run-shop-as-service"

    def test_init_staging(self, mock_cdf, mock_po):
        api = CogShopAPI(mock_cdf, mock_po, cog_shop_service="staging")
        assert api._cdf == mock_cdf
        assert api._po == mock_po
        assert api.cog_shop_service == "staging"
        url = api._shop_url_cshaas()
        assert url == "https://power-ops-api.staging.api.cognite.ai/power-ops-staging/run-shop-as-service"

    def test_init_prod(self, mock_cdf, mock_po):
        api = CogShopAPI(mock_cdf, mock_po, cog_shop_service="prod")
        assert api._cdf == mock_cdf
        assert api._po == mock_po
        assert api.cog_shop_service == "prod"
        url = api._shop_url_cshaas()
        assert url == "https://power-ops-api.api.cognite.ai/power-ops-staging/run-shop-as-service"


class TestShopScenarioReference:
    def test_validate_shop_scenario_reference_write_obj(self, cogshop_api):
        scenario_write = ShopScenarioWrite(
            space="space",
            external_id="scenario_ext_id",
            name="Scenario Name",
        )
        result = cogshop_api._validate_shop_scenario_reference(scenario_write)
        assert result is scenario_write

    def test_validate_shop_scenario_reference_external_id(self, cogshop_api):
        result = cogshop_api._validate_shop_scenario_reference("scenario_ext_id")
        assert result == "scenario_ext_id"

    def test_validate_shop_scenario_reference_invalid(self, cogshop_api):
        cogshop_api._po.shop_based_day_ahead_bid_process.shop_scenario.retrieve.return_value = None
        with pytest.raises(ValueError):
            cogshop_api._validate_shop_scenario_reference("invalid_id")


class TestShopCaseMethods:
    start_time: datetime = datetime.datetime(2023, 10, 1, 0, 0, tzinfo=datetime.timezone.utc)
    end_time: datetime = datetime.datetime(2023, 10, 2, 0, 0, tzinfo=datetime.timezone.utc)

    def test_prepare_shop_case(self, cogshop_api):
        shop_case = cogshop_api.prepare_shop_case(
            shop_file_list=[],
            shop_version="16.0.2",
            start_time=self.start_time,
            end_time=self.end_time,
            model_name="test_model",
            scenario_name="test_scenario",
            model_external_id="test_model_ext_id",
            scenario_external_id="test_scenario_ext_id",
            case_external_id="test_case_ext_id",
        )
        assert isinstance(shop_case, ShopCaseWrite)
        assert shop_case.space == "power_ops_instances"
        assert shop_case.external_id == "test_case_ext_id"
        assert shop_case.start_time == self.start_time
        assert shop_case.end_time == self.end_time
        assert shop_case.status == "default"
        assert shop_case.shop_files == []
        assert isinstance(shop_case.scenario, ShopScenarioWrite)
        assert shop_case.scenario.space == "power_ops_instances"
        assert shop_case.scenario.external_id == "test_scenario_ext_id"
        assert shop_case.scenario.name == "test_scenario"
        assert isinstance(shop_case.scenario.model, ShopModelWrite)
        assert shop_case.scenario.model.external_id == "test_model_ext_id"
        assert shop_case.scenario.model.name == "test_model"
        assert shop_case.scenario.model.shop_version == "16.0.2"


class TestStatus:
    @mock.patch("cognite.powerops.client.shop.cogshop_api.requests.get")
    def test_running_service_is_available_with_queue_counts(self, mock_get, cogshop_api):
        mock_get.return_value.json.return_value = {"status": "RUNNING", "todo": 2, "doing": 1, "todoList": []}

        result = cogshop_api.status()

        assert result == CogShopStatus(status="RUNNING", queued=2, running=1)
        assert result.is_available
        assert (
            mock_get.call_args.kwargs["url"]
            == "https://power-ops-api.staging.api.cognite.ai/power-ops-staging/shop/metrics"
        )

    @mock.patch("cognite.powerops.client.shop.cogshop_api.requests.get")
    def test_sends_cdf_credentials_and_timeout(self, mock_get, cogshop_api, mock_cdf):
        mock_cdf.config.credentials.authorization_header.return_value = ("Authorization", "Bearer token")
        mock_get.return_value.json.return_value = {"status": "RUNNING", "todo": 0, "doing": 0}

        cogshop_api.status(timeout=3.0)

        assert mock_get.call_args.kwargs["timeout"] == 3.0
        prepared = mock_get.call_args.kwargs["auth"](requests.Request("GET", "https://example.com").prepare())
        assert prepared.headers["Authorization"] == "Bearer token"

    @pytest.mark.parametrize("reported", ["DISABLED", "ERROR"])
    @mock.patch("cognite.powerops.client.shop.cogshop_api.requests.get")
    def test_service_reported_as_not_running_is_not_available(self, mock_get, cogshop_api, reported):
        mock_get.return_value.json.return_value = {"status": reported, "todo": 0, "doing": 0}

        result = cogshop_api.status()

        assert result.status == reported
        assert not result.is_available

    @mock.patch("cognite.powerops.client.shop.cogshop_api.requests.get")
    def test_unreachable_power_ops_api_is_reported_not_raised(self, mock_get, cogshop_api):
        mock_get.side_effect = requests.ConnectionError("connection refused")

        result = cogshop_api.status()

        assert result == CogShopStatus(status="UNREACHABLE", queued=0, running=0, detail="connection refused")
        assert result.http_status is None
        assert not result.is_available

    @pytest.mark.parametrize("code", [403, 503])
    @mock.patch("cognite.powerops.client.shop.cogshop_api.requests.get")
    def test_rejected_request_is_reported_with_its_http_status(self, mock_get, cogshop_api, code):
        mock_get.return_value.raise_for_status.side_effect = requests.HTTPError(
            f"{code} Error", response=mock.Mock(status_code=code)
        )

        result = cogshop_api.status()

        assert result.status == "UNREACHABLE"
        assert result.http_status == code
        assert result.detail == f"{code} Error"

    @mock.patch("cognite.powerops.client.shop.cogshop_api.requests.get")
    def test_non_json_answer_is_reported_as_unreachable(self, mock_get, cogshop_api):
        mock_get.return_value.json.side_effect = requests.JSONDecodeError("Expecting value", "", 0)

        result = cogshop_api.status()

        assert result.status == "UNREACHABLE"
        assert "Expecting value" in result.detail

    @mock.patch("cognite.powerops.client.shop.cogshop_api.requests.get")
    def test_answer_without_status_is_an_error(self, mock_get, cogshop_api):
        mock_get.return_value.json.return_value = {}

        result = cogshop_api.status()

        assert result.status == "ERROR"
        assert not result.is_available
        assert result.detail

    @mock.patch("cognite.powerops.client.shop.cogshop_api.requests.get")
    def test_answer_that_is_not_an_object_is_an_error(self, mock_get, cogshop_api):
        mock_get.return_value.json.return_value = ["RUNNING"]

        result = cogshop_api.status()

        assert result.status == "ERROR"
        assert result.detail

    @mock.patch("cognite.powerops.client.shop.cogshop_api.requests.get")
    def test_unknown_status_is_an_error_naming_the_value(self, mock_get, cogshop_api):
        mock_get.return_value.json.return_value = {"status": "running", "todo": 0, "doing": 0}

        result = cogshop_api.status()

        assert result.status == "ERROR"
        assert not result.is_available
        assert "'running'" in result.detail

    @pytest.mark.parametrize("todo", ["abc", None])
    @mock.patch("cognite.powerops.client.shop.cogshop_api.requests.get")
    def test_unreadable_queue_counts_are_an_error(self, mock_get, cogshop_api, todo):
        mock_get.return_value.json.return_value = {"status": "RUNNING", "todo": todo, "doing": 0}

        result = cogshop_api.status()

        assert result == CogShopStatus(status="ERROR", queued=0, running=0, detail=result.detail)
        assert result.detail

    def test_invalid_timeout_is_a_programming_error(self, cogshop_api, mock_cdf):
        mock_cdf.config.credentials.authorization_header.return_value = ("Authorization", "Bearer token")
        # requests.get is deliberately not mocked: urllib3 rejects the timeout before opening a connection.
        with (
            mock.patch.object(cogshop_api, "_power_ops_api_url", return_value="http://127.0.0.1:9"),
            pytest.raises(ValueError),
        ):
            cogshop_api.status(timeout=-1)
