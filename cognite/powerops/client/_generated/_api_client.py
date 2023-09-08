from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

from cognite.powerops.client._generated._api.benchmark_bids import BenchmarkBidsAPI
from cognite.powerops.client._generated._api.benchmark_process import BenchmarkProcessAPI
from cognite.powerops.client._generated._api.bid_matrix_generators import BidMatrixGeneratorsAPI
from cognite.powerops.client._generated._api.command_configs import CommandConfigsAPI
from cognite.powerops.client._generated._api.date_transformations import DateTransformationsAPI
from cognite.powerops.client._generated._api.day_ahead_bids import DayAheadBidsAPI
from cognite.powerops.client._generated._api.day_ahead_process import DayAheadProcessAPI
from cognite.powerops.client._generated._api.generators import GeneratorsAPI
from cognite.powerops.client._generated._api.input_time_series_mappings import InputTimeSeriesMappingsAPI
from cognite.powerops.client._generated._api.nord_pool_markets import NordPoolMarketsAPI
from cognite.powerops.client._generated._api.output_containers import OutputContainersAPI
from cognite.powerops.client._generated._api.output_mappings import OutputMappingsAPI
from cognite.powerops.client._generated._api.plants import PlantsAPI
from cognite.powerops.client._generated._api.price_areas import PriceAreasAPI
from cognite.powerops.client._generated._api.production_plan_time_series import ProductionPlanTimeSeriesAPI
from cognite.powerops.client._generated._api.reserve_scenarios import ReserveScenariosAPI
from cognite.powerops.client._generated._api.reservoirs import ReservoirsAPI
from cognite.powerops.client._generated._api.rkom_bid_combinations import RKOMBidCombinationsAPI
from cognite.powerops.client._generated._api.rkom_bids import RKOMBidsAPI
from cognite.powerops.client._generated._api.rkom_combination_bids import RKOMCombinationBidsAPI
from cognite.powerops.client._generated._api.rkom_markets import RKOMMarketsAPI
from cognite.powerops.client._generated._api.rkom_process import RKOMProcessAPI
from cognite.powerops.client._generated._api.scenario_mappings import ScenarioMappingsAPI
from cognite.powerops.client._generated._api.scenario_templates import ScenarioTemplatesAPI
from cognite.powerops.client._generated._api.scenarios import ScenariosAPI
from cognite.powerops.client._generated._api.shop_transformations import ShopTransformationsAPI
from cognite.powerops.client._generated._api.value_transformations import ValueTransformationsAPI
from cognite.powerops.client._generated._api.watercourse_shops import WatercourseShopsAPI
from cognite.powerops.client._generated._api.watercourses import WatercoursesAPI


class BenchmarkAPIs:
    """
    BenchmarkAPIs

    Data Model:
        space: power-ops
        externalId: benchmarkMarket
        version: 1

    """

    def __init__(self, client: CogniteClient):
        self.benchmark_bids = BenchmarkBidsAPI(client)
        self.benchmark_process = BenchmarkProcessAPI(client)
        self.date_transformations = DateTransformationsAPI(client)
        self.input_time_series_mappings = InputTimeSeriesMappingsAPI(client)
        self.nord_pool_markets = NordPoolMarketsAPI(client)
        self.production_plan_time_series = ProductionPlanTimeSeriesAPI(client)
        self.scenario_mappings = ScenarioMappingsAPI(client)
        self.shop_transformations = ShopTransformationsAPI(client)
        self.value_transformations = ValueTransformationsAPI(client)


class CogShopAPIs:
    """
    CogShopAPIs

    Data Model:
        space: power-ops
        externalId: cogshop
        version: 1

    """

    def __init__(self, client: CogniteClient):
        self.command_configs = CommandConfigsAPI(client)
        self.input_time_series_mappings = InputTimeSeriesMappingsAPI(client)
        self.output_containers = OutputContainersAPI(client)
        self.output_mappings = OutputMappingsAPI(client)
        self.scenarios = ScenariosAPI(client)
        self.scenario_mappings = ScenarioMappingsAPI(client)
        self.scenario_templates = ScenarioTemplatesAPI(client)
        self.value_transformations = ValueTransformationsAPI(client)


class DayAheadAPIs:
    """
    DayAheadAPIs

    Data Model:
        space: power-ops
        externalId: dayaheadMarket
        version: 1

    """

    def __init__(self, client: CogniteClient):
        self.bid_matrix_generators = BidMatrixGeneratorsAPI(client)
        self.date_transformations = DateTransformationsAPI(client)
        self.day_ahead_bids = DayAheadBidsAPI(client)
        self.day_ahead_process = DayAheadProcessAPI(client)
        self.input_time_series_mappings = InputTimeSeriesMappingsAPI(client)
        self.nord_pool_markets = NordPoolMarketsAPI(client)
        self.scenario_mappings = ScenarioMappingsAPI(client)
        self.shop_transformations = ShopTransformationsAPI(client)
        self.value_transformations = ValueTransformationsAPI(client)


class ProductionAPIs:
    """
    ProductionAPIs

    Data Model:
        space: power-ops
        externalId: production
        version: 1

    """

    def __init__(self, client: CogniteClient):
        self.generators = GeneratorsAPI(client)
        self.plants = PlantsAPI(client)
        self.price_areas = PriceAreasAPI(client)
        self.reservoirs = ReservoirsAPI(client)
        self.watercourses = WatercoursesAPI(client)
        self.watercourse_shops = WatercourseShopsAPI(client)


class RKOMMarketAPIs:
    """
    RKOMMarketAPIs

    Data Model:
        space: power-ops
        externalId: rkomMarket
        version: 1

    """

    def __init__(self, client: CogniteClient):
        self.date_transformations = DateTransformationsAPI(client)
        self.input_time_series_mappings = InputTimeSeriesMappingsAPI(client)
        self.rkom_bids = RKOMBidsAPI(client)
        self.rkom_bid_combinations = RKOMBidCombinationsAPI(client)
        self.rkom_combination_bids = RKOMCombinationBidsAPI(client)
        self.rkom_markets = RKOMMarketsAPI(client)
        self.rkom_process = RKOMProcessAPI(client)
        self.reserve_scenarios = ReserveScenariosAPI(client)
        self.scenario_mappings = ScenarioMappingsAPI(client)
        self.shop_transformations = ShopTransformationsAPI(client)
        self.value_transformations = ValueTransformationsAPI(client)


class GeneratedPowerOpsClient:
    """
    GeneratedPowerOpsClient

    Generated with:
        pygen = 0.17.7
        cognite-sdk = 6.21.1
        pydantic = 2.3.0

    """

    def __init__(self, config_or_client: CogniteClient | ClientConfig):
        if isinstance(config_or_client, CogniteClient):
            client = config_or_client
        elif isinstance(config_or_client, ClientConfig):
            client = CogniteClient(config_or_client)
        else:
            raise ValueError(f"Expected CogniteClient or ClientConfig, got {type(config_or_client)}")
        self.benchmark = BenchmarkAPIs(client)
        self.cog_shop = CogShopAPIs(client)
        self.day_ahead = DayAheadAPIs(client)
        self.production = ProductionAPIs(client)
        self.rkom_market = RKOMMarketAPIs(client)

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> GeneratedPowerOpsClient:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> GeneratedPowerOpsClient:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)
