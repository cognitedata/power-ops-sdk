from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials

from ._api.benchmark_bid import BenchmarkBidAPI
from ._api.benchmark_process import BenchmarkProcessAPI
from ._api.bid_curves import BidCurvesAPI
from ._api.bid_document_header import BidDocumentHeaderAPI
from ._api.bid_matrix_generator import BidMatrixGeneratorAPI
from ._api.bid_time_series import BidTimeSeriesAPI
from ._api.command_config import CommandConfigAPI
from ._api.date_time_interval import DateTimeIntervalAPI
from ._api.date_transformation import DateTransformationAPI
from ._api.day_ahead_bid import DayAheadBidAPI
from ._api.day_ahead_process import DayAheadProcessAPI
from ._api.duration import DurationAPI
from ._api.generator import GeneratorAPI
from ._api.input_time_series_mapping import InputTimeSeriesMappingAPI
from ._api.market_agreement import MarketAgreementAPI
from ._api.market_participant import MarketParticipantAPI
from ._api.mba_domain import MBADomainAPI
from ._api.nord_pool_market import NordPoolMarketAPI
from ._api.output_container import OutputContainerAPI
from ._api.output_mapping import OutputMappingAPI
from ._api.periods import PeriodsAPI
from ._api.plant import PlantAPI
from ._api.point import PointAPI
from ._api.price_area import PriceAreaAPI
from ._api.production_plan_time_series import ProductionPlanTimeSeriesAPI
from ._api.reason import ReasonAPI
from ._api.reserve_bid import ReserveBidAPI
from ._api.reserve_bid_time_series import ReserveBidTimeSeriesAPI
from ._api.reserve_scenario import ReserveScenarioAPI
from ._api.reservoir import ReservoirAPI
from ._api.rkom_bid import RKOMBidAPI
from ._api.rkom_bid_combination import RKOMBidCombinationAPI
from ._api.rkom_combination_bid import RKOMCombinationBidAPI
from ._api.rkom_market import RKOMMarketAPI
from ._api.rkom_process import RKOMProcessAPI
from ._api.scenario import ScenarioAPI
from ._api.scenario_mapping import ScenarioMappingAPI
from ._api.scenario_template import ScenarioTemplateAPI
from ._api.series import SeriesAPI
from ._api.shop_transformation import ShopTransformationAPI
from ._api.time_interval import TimeIntervalAPI
from ._api.value_transformation import ValueTransformationAPI
from ._api.watercourse import WatercourseAPI
from ._api.watercourse_shop import WatercourseShopAPI


class AFRRAPIs:
    """
    AFRRAPIs

    Data Model:
        space: power-ops
        externalId: afrrMarket
        version: 1

    """

    def __init__(self, client: CogniteClient):
        self.bid_time_series = BidTimeSeriesAPI(client, dm.ViewId("power-ops", "BidTimeSeries", "2c1787140841fd"))
        self.date_time_interval = DateTimeIntervalAPI(
            client, dm.ViewId("power-ops", "DateTimeInterval", "18f6639083458b")
        )
        self.duration = DurationAPI(client, dm.ViewId("power-ops", "Duration", "7433a3f6ac2be0"))
        self.mba_domain = MBADomainAPI(client, dm.ViewId("power-ops", "MarketAgreement", "815d42dc6e008d"))
        self.market_agreement = MarketAgreementAPI(
            client, dm.ViewId("power-ops", "MarketParticipant", "8c47d7b03faeda")
        )
        self.market_participant = MarketParticipantAPI(client, dm.ViewId("power-ops", "MBADomain", "9ac70d436c3313"))
        self.point = PointAPI(client, dm.ViewId("power-ops", "Point", "791cb15b0ae9e1"))
        self.reason = ReasonAPI(client, dm.ViewId("power-ops", "Reason", "d064355f848186"))
        self.reserve_bid = ReserveBidAPI(client, dm.ViewId("power-ops", "ReserveBid", "dfb50d1e05d2e5"))
        self.series = SeriesAPI(client, dm.ViewId("power-ops", "Series", "59d189398e78be"))


class BenchmarkAPIs:
    """
    BenchmarkAPIs

    Data Model:
        space: power-ops
        externalId: benchmarkMarket
        version: 1

    """

    def __init__(self, client: CogniteClient):
        self.benchmark_bid = BenchmarkBidAPI(client, dm.ViewId("power-ops", "BenchmarkBid", "cd2ea6a54b92a6"))
        self.benchmark_process = BenchmarkProcessAPI(
            client, dm.ViewId("power-ops", "BenchmarkProcess", "3c3a0761a5f084")
        )
        self.date_transformation = DateTransformationAPI(
            client, dm.ViewId("power-ops", "DateTransformation", "a7c71305ba1288")
        )
        self.input_time_series_mapping = InputTimeSeriesMappingAPI(
            client, dm.ViewId("power-ops", "InputTimeSeriesMapping", "2426123a688e61")
        )
        self.nord_pool_market = NordPoolMarketAPI(client, dm.ViewId("power-ops", "NordPoolMarket", "88c86032b9ac9c"))
        self.production_plan_time_series = ProductionPlanTimeSeriesAPI(
            client, dm.ViewId("power-ops", "ProductionPlanTimeSeries", "ca7ffcb6f63d3f")
        )
        self.scenario_mapping = ScenarioMappingAPI(client, dm.ViewId("power-ops", "ScenarioMapping", "e65d4465699308"))
        self.shop_transformation = ShopTransformationAPI(
            client, dm.ViewId("power-ops", "ShopTransformation", "a74d706d1bda99")
        )
        self.value_transformation = ValueTransformationAPI(
            client, dm.ViewId("power-ops", "ValueTransformation", "acd34e005f1986")
        )


class CapacityBidAPIs:
    """
    CapacityBidAPIs

    Data Model:
        space: power-ops
        externalId: capacityBid
        version: 1

    """

    def __init__(self, client: CogniteClient):
        self.bid_curves = BidCurvesAPI(client, dm.ViewId("power-ops", "BidCurves", "0_1"))
        self.bid_document_header = BidDocumentHeaderAPI(client, dm.ViewId("power-ops", "BidDocumentHeader", "0_1"))
        self.periods = PeriodsAPI(client, dm.ViewId("power-ops", "Periods", "0_1"))
        self.reserve_bid_time_series = ReserveBidTimeSeriesAPI(
            client, dm.ViewId("power-ops", "ReserveBidTimeSeries", "0_1")
        )
        self.time_interval = TimeIntervalAPI(client, dm.ViewId("power-ops", "TimeInterval", "0_1"))


class CogShopAPIs:
    """
    CogShopAPIs

    Data Model:
        space: power-ops
        externalId: cogshop
        version: 1

    """

    def __init__(self, client: CogniteClient):
        self.command_config = CommandConfigAPI(client, dm.ViewId("power-ops", "CommandConfig", "128f1e0abfc7c5"))
        self.input_time_series_mapping = InputTimeSeriesMappingAPI(
            client, dm.ViewId("power-ops", "InputTimeSeriesMapping", "39f0e93d6b2bc1")
        )
        self.output_container = OutputContainerAPI(client, dm.ViewId("power-ops", "OutputContainer", "ad054c0f19ea87"))
        self.output_mapping = OutputMappingAPI(client, dm.ViewId("power-ops", "OutputMapping", "58e6e8f0dadecc"))
        self.scenario = ScenarioAPI(client, dm.ViewId("power-ops", "Scenario", "eb6cd945bd1400"))
        self.scenario_mapping = ScenarioMappingAPI(client, dm.ViewId("power-ops", "ScenarioMapping", "1cd399b3faffc4"))
        self.scenario_template = ScenarioTemplateAPI(
            client, dm.ViewId("power-ops", "ScenarioTemplate", "77579c65a8cdf9")
        )
        self.value_transformation = ValueTransformationAPI(
            client, dm.ViewId("power-ops", "ValueTransformation", "1b641fef1e0a83")
        )


class DayAheadAPIs:
    """
    DayAheadAPIs

    Data Model:
        space: power-ops
        externalId: dayaheadMarket
        version: 1

    """

    def __init__(self, client: CogniteClient):
        self.bid_matrix_generator = BidMatrixGeneratorAPI(
            client, dm.ViewId("power-ops", "BidMatrixGenerator", "98145498689f24")
        )
        self.date_transformation = DateTransformationAPI(
            client, dm.ViewId("power-ops", "DateTransformation", "78995d48b59c57")
        )
        self.day_ahead_bid = DayAheadBidAPI(client, dm.ViewId("power-ops", "DayAheadBid", "bd0768f04d3708"))
        self.day_ahead_process = DayAheadProcessAPI(client, dm.ViewId("power-ops", "DayAheadProcess", "dd1bf62feefc9a"))
        self.input_time_series_mapping = InputTimeSeriesMappingAPI(
            client, dm.ViewId("power-ops", "InputTimeSeriesMapping", "9532a47c052eca")
        )
        self.nord_pool_market = NordPoolMarketAPI(client, dm.ViewId("power-ops", "NordPoolMarket", "919be6b14f829d"))
        self.scenario_mapping = ScenarioMappingAPI(client, dm.ViewId("power-ops", "ScenarioMapping", "7d1c17ee79d79d"))
        self.shop_transformation = ShopTransformationAPI(
            client, dm.ViewId("power-ops", "ShopTransformation", "2dd4c9f8e072b6")
        )
        self.value_transformation = ValueTransformationAPI(
            client, dm.ViewId("power-ops", "ValueTransformation", "72f3a548b93c67")
        )


class ProductionAPIs:
    """
    ProductionAPIs

    Data Model:
        space: power-ops
        externalId: production
        version: 1

    """

    def __init__(self, client: CogniteClient):
        self.generator = GeneratorAPI(client, dm.ViewId("power-ops", "Generator", "9178931bbaac71"))
        self.plant = PlantAPI(client, dm.ViewId("power-ops", "Plant", "836dcb3f5da1df"))
        self.price_area = PriceAreaAPI(client, dm.ViewId("power-ops", "PriceArea", "6849ae787cd368"))
        self.reservoir = ReservoirAPI(client, dm.ViewId("power-ops", "Reservoir", "3c822b0c3d68f7"))
        self.watercourse = WatercourseAPI(client, dm.ViewId("power-ops", "Watercourse", "96f5170f35ef70"))
        self.watercourse_shop = WatercourseShopAPI(client, dm.ViewId("power-ops", "WatercourseShop", "4b5321b1fccd06"))


class RKOMMarketAPIs:
    """
    RKOMMarketAPIs

    Data Model:
        space: power-ops
        externalId: rkomMarket
        version: 1

    """

    def __init__(self, client: CogniteClient):
        self.date_transformation = DateTransformationAPI(
            client, dm.ViewId("power-ops", "DateTransformation", "13820c127c31c0")
        )
        self.input_time_series_mapping = InputTimeSeriesMappingAPI(
            client, dm.ViewId("power-ops", "InputTimeSeriesMapping", "7fce8483d52568")
        )
        self.rkom_bid = RKOMBidAPI(client, dm.ViewId("power-ops", "ReserveScenario", "e971c10bd1e893"))
        self.rkom_bid_combination = RKOMBidCombinationAPI(client, dm.ViewId("power-ops", "RKOMBid", "5c1a2ba06aa41f"))
        self.rkom_combination_bid = RKOMCombinationBidAPI(
            client, dm.ViewId("power-ops", "RKOMBidCombination", "b8c2faf6e35abe")
        )
        self.rkom_market = RKOMMarketAPI(client, dm.ViewId("power-ops", "RKOMCombinationBid", "a81dbcbbcd26de"))
        self.rkom_process = RKOMProcessAPI(client, dm.ViewId("power-ops", "RKOMMarket", "c362cd4abb3d4e"))
        self.reserve_scenario = ReserveScenarioAPI(client, dm.ViewId("power-ops", "RKOMProcess", "268dee7a04a5c3"))
        self.scenario_mapping = ScenarioMappingAPI(client, dm.ViewId("power-ops", "ScenarioMapping", "2b5b1f6fa4f53d"))
        self.shop_transformation = ShopTransformationAPI(
            client, dm.ViewId("power-ops", "ShopTransformation", "d0a6c80379e55b")
        )
        self.value_transformation = ValueTransformationAPI(
            client, dm.ViewId("power-ops", "ValueTransformation", "894c9c530a1c1d")
        )


class GeneratedPowerOpsClient:
    """
    GeneratedPowerOpsClient

    Generated with:
        pygen = 0.27.1
        cognite-sdk = 6.37.0
        pydantic = 2.4.2

    """

    def __init__(self, config_or_client: CogniteClient | ClientConfig):
        if isinstance(config_or_client, CogniteClient):
            client = config_or_client
        elif isinstance(config_or_client, ClientConfig):
            client = CogniteClient(config_or_client)
        else:
            raise ValueError(f"Expected CogniteClient or ClientConfig, got {type(config_or_client)}")
        self.afrr = AFRRAPIs(client)
        self.benchmark = BenchmarkAPIs(client)
        self.capacity_bid = CapacityBidAPIs(client)
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
