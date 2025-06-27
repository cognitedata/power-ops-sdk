import datetime
from unittest import mock

import pytest

from cognite.powerops.client._generated.v1.data_classes import ShopCaseWrite, ShopModelWrite, ShopScenarioWrite
from cognite.powerops.client.shop.cogshop_api import CogShopAPI


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
