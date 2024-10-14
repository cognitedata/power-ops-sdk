from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any, Sequence

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.data_classes import TimeSeriesList
from cognite.client.credentials import OAuthClientCredentials

from ._api.alert import AlertAPI
from ._api.benchmarking_calculation_input import BenchmarkingCalculationInputAPI
from ._api.benchmarking_calculation_output import BenchmarkingCalculationOutputAPI
from ._api.benchmarking_configuration_day_ahead import BenchmarkingConfigurationDayAheadAPI
from ._api.benchmarking_production_obligation_day_ahead import BenchmarkingProductionObligationDayAheadAPI
from ._api.benchmarking_result_day_ahead import BenchmarkingResultDayAheadAPI
from ._api.benchmarking_shop_case import BenchmarkingShopCaseAPI
from ._api.benchmarking_task_dispatcher_input_day_ahead import BenchmarkingTaskDispatcherInputDayAheadAPI
from ._api.benchmarking_task_dispatcher_output_day_ahead import BenchmarkingTaskDispatcherOutputDayAheadAPI
from ._api.bid_configuration_day_ahead import BidConfigurationDayAheadAPI
from ._api.bid_document import BidDocumentAPI
from ._api.bid_document_afrr import BidDocumentAFRRAPI
from ._api.bid_document_day_ahead import BidDocumentDayAheadAPI
from ._api.bid_matrix import BidMatrixAPI
from ._api.bid_matrix_information import BidMatrixInformationAPI
from ._api.bid_row import BidRowAPI
from ._api.date_specification import DateSpecificationAPI
from ._api.function_input import FunctionInputAPI
from ._api.function_output import FunctionOutputAPI
from ._api.generator import GeneratorAPI
from ._api.generator_efficiency_curve import GeneratorEfficiencyCurveAPI
from ._api.market_configuration import MarketConfigurationAPI
from ._api.multi_scenario_partial_bid_matrix_calculation_input import MultiScenarioPartialBidMatrixCalculationInputAPI
from ._api.partial_bid_configuration import PartialBidConfigurationAPI
from ._api.partial_bid_matrix_calculation_input import PartialBidMatrixCalculationInputAPI
from ._api.partial_bid_matrix_calculation_output import PartialBidMatrixCalculationOutputAPI
from ._api.partial_bid_matrix_information import PartialBidMatrixInformationAPI
from ._api.partial_bid_matrix_information_with_scenarios import PartialBidMatrixInformationWithScenariosAPI
from ._api.plant import PlantAPI
from ._api.plant_information import PlantInformationAPI
from ._api.plant_water_value_based import PlantWaterValueBasedAPI
from ._api.power_asset import PowerAssetAPI
from ._api.price_area import PriceAreaAPI
from ._api.price_area_afrr import PriceAreaAFRRAPI
from ._api.price_area_day_ahead import PriceAreaDayAheadAPI
from ._api.price_area_information import PriceAreaInformationAPI
from ._api.price_production import PriceProductionAPI
from ._api.shop_attribute_mapping import ShopAttributeMappingAPI
from ._api.shop_based_partial_bid_configuration import ShopBasedPartialBidConfigurationAPI
from ._api.shop_case import ShopCaseAPI
from ._api.shop_commands import ShopCommandsAPI
from ._api.shop_file import ShopFileAPI
from ._api.shop_model import ShopModelAPI
from ._api.shop_model_with_assets import ShopModelWithAssetsAPI
from ._api.shop_output_time_series_definition import ShopOutputTimeSeriesDefinitionAPI
from ._api.shop_penalty_report import ShopPenaltyReportAPI
from ._api.shop_preprocessor_input import ShopPreprocessorInputAPI
from ._api.shop_preprocessor_output import ShopPreprocessorOutputAPI
from ._api.shop_result import ShopResultAPI
from ._api.shop_scenario import ShopScenarioAPI
from ._api.shop_scenario_set import ShopScenarioSetAPI
from ._api.shop_time_series import ShopTimeSeriesAPI
from ._api.shop_trigger_input import ShopTriggerInputAPI
from ._api.shop_trigger_output import ShopTriggerOutputAPI
from ._api.task_dispatcher_input import TaskDispatcherInputAPI
from ._api.task_dispatcher_output import TaskDispatcherOutputAPI
from ._api.total_bid_matrix_calculation_input import TotalBidMatrixCalculationInputAPI
from ._api.total_bid_matrix_calculation_output import TotalBidMatrixCalculationOutputAPI
from ._api.turbine_efficiency_curve import TurbineEfficiencyCurveAPI
from ._api.water_value_based_partial_bid_configuration import WaterValueBasedPartialBidConfigurationAPI
from ._api.water_value_based_partial_bid_matrix_calculation_input import WaterValueBasedPartialBidMatrixCalculationInputAPI
from ._api.watercourse import WatercourseAPI
from ._api._core import SequenceNotStr, GraphQLQueryResponse
from .data_classes._core import DEFAULT_INSTANCE_SPACE, GraphQLList
from . import data_classes


class BenchmarkingDayAheadAPIs:
    """
    BenchmarkingDayAheadAPIs

    Data Model:
        space: power_ops_core
        externalId: compute_BenchmarkingDayAhead
        version: 1

    """

    def __init__(self, client: CogniteClient):
        self._client = client

        self.alert = AlertAPI(client)
        self.benchmarking_calculation_input = BenchmarkingCalculationInputAPI(client)
        self.benchmarking_calculation_output = BenchmarkingCalculationOutputAPI(client)
        self.benchmarking_configuration_day_ahead = BenchmarkingConfigurationDayAheadAPI(client)
        self.benchmarking_production_obligation_day_ahead = BenchmarkingProductionObligationDayAheadAPI(client)
        self.benchmarking_result_day_ahead = BenchmarkingResultDayAheadAPI(client)
        self.benchmarking_shop_case = BenchmarkingShopCaseAPI(client)
        self.benchmarking_task_dispatcher_input_day_ahead = BenchmarkingTaskDispatcherInputDayAheadAPI(client)
        self.benchmarking_task_dispatcher_output_day_ahead = BenchmarkingTaskDispatcherOutputDayAheadAPI(client)
        self.bid_configuration_day_ahead = BidConfigurationDayAheadAPI(client)
        self.date_specification = DateSpecificationAPI(client)
        self.function_input = FunctionInputAPI(client)
        self.function_output = FunctionOutputAPI(client)
        self.market_configuration = MarketConfigurationAPI(client)
        self.partial_bid_configuration = PartialBidConfigurationAPI(client)
        self.power_asset = PowerAssetAPI(client)
        self.price_area = PriceAreaAPI(client)
        self.price_area_day_ahead = PriceAreaDayAheadAPI(client)
        self.shop_attribute_mapping = ShopAttributeMappingAPI(client)
        self.shop_case = ShopCaseAPI(client)
        self.shop_commands = ShopCommandsAPI(client)
        self.shop_file = ShopFileAPI(client)
        self.shop_model = ShopModelAPI(client)
        self.shop_model_with_assets = ShopModelWithAssetsAPI(client)
        self.shop_output_time_series_definition = ShopOutputTimeSeriesDefinitionAPI(client)
        self.shop_preprocessor_input = ShopPreprocessorInputAPI(client)
        self.shop_result = ShopResultAPI(client)
        self.shop_scenario = ShopScenarioAPI(client)
        self.shop_time_series = ShopTimeSeriesAPI(client)
        self.shop_trigger_input = ShopTriggerInputAPI(client)

    def graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> GraphQLList:
        """Execute a GraphQl query against the compute_BenchmarkingDayAhead data model.

            Args:
                query (str): The GraphQL query to issue.
                variables (dict[str, Any] | None): An optional dict of variables to pass to the query.
        """
        data_model_id = dm.DataModelId("power_ops_core", "compute_BenchmarkingDayAhead", "1")
        result = self._client.data_modeling.graphql.query(data_model_id, query, variables)
        return GraphQLQueryResponse(data_model_id).parse(result)


class ShopBasedDayAheadBidProcesAPIs:
    """
    ShopBasedDayAheadBidProcesAPIs

    Data Model:
        space: power_ops_core
        externalId: compute_ShopBasedDayAhead
        version: 1

    """

    def __init__(self, client: CogniteClient):
        self._client = client

        self.alert = AlertAPI(client)
        self.bid_configuration_day_ahead = BidConfigurationDayAheadAPI(client)
        self.bid_matrix = BidMatrixAPI(client)
        self.date_specification = DateSpecificationAPI(client)
        self.function_input = FunctionInputAPI(client)
        self.function_output = FunctionOutputAPI(client)
        self.market_configuration = MarketConfigurationAPI(client)
        self.multi_scenario_partial_bid_matrix_calculation_input = MultiScenarioPartialBidMatrixCalculationInputAPI(client)
        self.partial_bid_configuration = PartialBidConfigurationAPI(client)
        self.partial_bid_matrix_calculation_input = PartialBidMatrixCalculationInputAPI(client)
        self.partial_bid_matrix_calculation_output = PartialBidMatrixCalculationOutputAPI(client)
        self.power_asset = PowerAssetAPI(client)
        self.price_area = PriceAreaAPI(client)
        self.price_area_day_ahead = PriceAreaDayAheadAPI(client)
        self.price_production = PriceProductionAPI(client)
        self.shop_attribute_mapping = ShopAttributeMappingAPI(client)
        self.shop_based_partial_bid_configuration = ShopBasedPartialBidConfigurationAPI(client)
        self.shop_case = ShopCaseAPI(client)
        self.shop_commands = ShopCommandsAPI(client)
        self.shop_file = ShopFileAPI(client)
        self.shop_model = ShopModelAPI(client)
        self.shop_output_time_series_definition = ShopOutputTimeSeriesDefinitionAPI(client)
        self.shop_preprocessor_input = ShopPreprocessorInputAPI(client)
        self.shop_preprocessor_output = ShopPreprocessorOutputAPI(client)
        self.shop_result = ShopResultAPI(client)
        self.shop_scenario = ShopScenarioAPI(client)
        self.shop_scenario_set = ShopScenarioSetAPI(client)
        self.shop_time_series = ShopTimeSeriesAPI(client)
        self.shop_trigger_input = ShopTriggerInputAPI(client)
        self.shop_trigger_output = ShopTriggerOutputAPI(client)
        self.task_dispatcher_input = TaskDispatcherInputAPI(client)
        self.task_dispatcher_output = TaskDispatcherOutputAPI(client)

    def graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> GraphQLList:
        """Execute a GraphQl query against the compute_ShopBasedDayAhead data model.

            Args:
                query (str): The GraphQL query to issue.
                variables (dict[str, Any] | None): An optional dict of variables to pass to the query.
        """
        data_model_id = dm.DataModelId("power_ops_core", "compute_ShopBasedDayAhead", "1")
        result = self._client.data_modeling.graphql.query(data_model_id, query, variables)
        return GraphQLQueryResponse(data_model_id).parse(result)


class TotalBidMatrixCalculationAPIs:
    """
    TotalBidMatrixCalculationAPIs

    Data Model:
        space: power_ops_core
        externalId: compute_TotalBidMatrixCalculation
        version: 1

    """

    def __init__(self, client: CogniteClient):
        self._client = client

        self.alert = AlertAPI(client)
        self.bid_configuration_day_ahead = BidConfigurationDayAheadAPI(client)
        self.bid_document = BidDocumentAPI(client)
        self.bid_document_day_ahead = BidDocumentDayAheadAPI(client)
        self.bid_matrix = BidMatrixAPI(client)
        self.bid_matrix_information = BidMatrixInformationAPI(client)
        self.date_specification = DateSpecificationAPI(client)
        self.function_input = FunctionInputAPI(client)
        self.function_output = FunctionOutputAPI(client)
        self.market_configuration = MarketConfigurationAPI(client)
        self.partial_bid_configuration = PartialBidConfigurationAPI(client)
        self.partial_bid_matrix_information = PartialBidMatrixInformationAPI(client)
        self.power_asset = PowerAssetAPI(client)
        self.price_area = PriceAreaAPI(client)
        self.price_area_day_ahead = PriceAreaDayAheadAPI(client)
        self.price_production = PriceProductionAPI(client)
        self.shop_attribute_mapping = ShopAttributeMappingAPI(client)
        self.shop_case = ShopCaseAPI(client)
        self.shop_commands = ShopCommandsAPI(client)
        self.shop_file = ShopFileAPI(client)
        self.shop_model = ShopModelAPI(client)
        self.shop_output_time_series_definition = ShopOutputTimeSeriesDefinitionAPI(client)
        self.shop_result = ShopResultAPI(client)
        self.shop_scenario = ShopScenarioAPI(client)
        self.shop_time_series = ShopTimeSeriesAPI(client)
        self.total_bid_matrix_calculation_input = TotalBidMatrixCalculationInputAPI(client)
        self.total_bid_matrix_calculation_output = TotalBidMatrixCalculationOutputAPI(client)

    def graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> GraphQLList:
        """Execute a GraphQl query against the compute_TotalBidMatrixCalculation data model.

            Args:
                query (str): The GraphQL query to issue.
                variables (dict[str, Any] | None): An optional dict of variables to pass to the query.
        """
        data_model_id = dm.DataModelId("power_ops_core", "compute_TotalBidMatrixCalculation", "1")
        result = self._client.data_modeling.graphql.query(data_model_id, query, variables)
        return GraphQLQueryResponse(data_model_id).parse(result)


class WaterValueBasedDayAheadBidProcesAPIs:
    """
    WaterValueBasedDayAheadBidProcesAPIs

    Data Model:
        space: power_ops_core
        externalId: compute_WaterValueBasedDayAheadBid
        version: 1

    """

    def __init__(self, client: CogniteClient):
        self._client = client

        self.alert = AlertAPI(client)
        self.bid_configuration_day_ahead = BidConfigurationDayAheadAPI(client)
        self.bid_matrix = BidMatrixAPI(client)
        self.date_specification = DateSpecificationAPI(client)
        self.function_input = FunctionInputAPI(client)
        self.function_output = FunctionOutputAPI(client)
        self.generator = GeneratorAPI(client)
        self.generator_efficiency_curve = GeneratorEfficiencyCurveAPI(client)
        self.market_configuration = MarketConfigurationAPI(client)
        self.partial_bid_configuration = PartialBidConfigurationAPI(client)
        self.partial_bid_matrix_calculation_input = PartialBidMatrixCalculationInputAPI(client)
        self.partial_bid_matrix_calculation_output = PartialBidMatrixCalculationOutputAPI(client)
        self.plant = PlantAPI(client)
        self.plant_water_value_based = PlantWaterValueBasedAPI(client)
        self.power_asset = PowerAssetAPI(client)
        self.price_area = PriceAreaAPI(client)
        self.price_area_day_ahead = PriceAreaDayAheadAPI(client)
        self.task_dispatcher_input = TaskDispatcherInputAPI(client)
        self.task_dispatcher_output = TaskDispatcherOutputAPI(client)
        self.turbine_efficiency_curve = TurbineEfficiencyCurveAPI(client)
        self.water_value_based_partial_bid_configuration = WaterValueBasedPartialBidConfigurationAPI(client)
        self.water_value_based_partial_bid_matrix_calculation_input = WaterValueBasedPartialBidMatrixCalculationInputAPI(client)

    def graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> GraphQLList:
        """Execute a GraphQl query against the compute_WaterValueBasedDayAheadBid data model.

            Args:
                query (str): The GraphQL query to issue.
                variables (dict[str, Any] | None): An optional dict of variables to pass to the query.
        """
        data_model_id = dm.DataModelId("power_ops_core", "compute_WaterValueBasedDayAheadBid", "1")
        result = self._client.data_modeling.graphql.query(data_model_id, query, variables)
        return GraphQLQueryResponse(data_model_id).parse(result)


class DayAheadConfigurationAPIs:
    """
    DayAheadConfigurationAPIs

    Data Model:
        space: power_ops_core
        externalId: config_DayAheadConfiguration
        version: 1

    """

    def __init__(self, client: CogniteClient):
        self._client = client

        self.bid_configuration_day_ahead = BidConfigurationDayAheadAPI(client)
        self.date_specification = DateSpecificationAPI(client)
        self.generator = GeneratorAPI(client)
        self.generator_efficiency_curve = GeneratorEfficiencyCurveAPI(client)
        self.market_configuration = MarketConfigurationAPI(client)
        self.partial_bid_configuration = PartialBidConfigurationAPI(client)
        self.plant = PlantAPI(client)
        self.plant_information = PlantInformationAPI(client)
        self.plant_water_value_based = PlantWaterValueBasedAPI(client)
        self.power_asset = PowerAssetAPI(client)
        self.price_area = PriceAreaAPI(client)
        self.price_area_day_ahead = PriceAreaDayAheadAPI(client)
        self.shop_attribute_mapping = ShopAttributeMappingAPI(client)
        self.shop_based_partial_bid_configuration = ShopBasedPartialBidConfigurationAPI(client)
        self.shop_commands = ShopCommandsAPI(client)
        self.shop_file = ShopFileAPI(client)
        self.shop_model = ShopModelAPI(client)
        self.shop_output_time_series_definition = ShopOutputTimeSeriesDefinitionAPI(client)
        self.shop_scenario = ShopScenarioAPI(client)
        self.shop_scenario_set = ShopScenarioSetAPI(client)
        self.turbine_efficiency_curve = TurbineEfficiencyCurveAPI(client)
        self.water_value_based_partial_bid_configuration = WaterValueBasedPartialBidConfigurationAPI(client)

    def graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> GraphQLList:
        """Execute a GraphQl query against the config_DayAheadConfiguration data model.

            Args:
                query (str): The GraphQL query to issue.
                variables (dict[str, Any] | None): An optional dict of variables to pass to the query.
        """
        data_model_id = dm.DataModelId("power_ops_core", "config_DayAheadConfiguration", "1")
        result = self._client.data_modeling.graphql.query(data_model_id, query, variables)
        return GraphQLQueryResponse(data_model_id).parse(result)


class AFRRBidAPIs:
    """
    AFRRBidAPIs

    Data Model:
        space: power_ops_core
        externalId: frontend_AFRRBid
        version: 1

    """

    def __init__(self, client: CogniteClient):
        self._client = client

        self.alert = AlertAPI(client)
        self.bid_document = BidDocumentAPI(client)
        self.bid_document_afrr = BidDocumentAFRRAPI(client)
        self.bid_row = BidRowAPI(client)
        self.power_asset = PowerAssetAPI(client)
        self.price_area = PriceAreaAPI(client)
        self.price_area_afrr = PriceAreaAFRRAPI(client)

    def graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> GraphQLList:
        """Execute a GraphQl query against the frontend_AFRRBid data model.

            Args:
                query (str): The GraphQL query to issue.
                variables (dict[str, Any] | None): An optional dict of variables to pass to the query.
        """
        data_model_id = dm.DataModelId("power_ops_core", "frontend_AFRRBid", "1")
        result = self._client.data_modeling.graphql.query(data_model_id, query, variables)
        return GraphQLQueryResponse(data_model_id).parse(result)


class PowerAssetAPIs:
    """
    PowerAssetAPIs

    Data Model:
        space: power_ops_core
        externalId: frontend_Asset
        version: 1

    """

    def __init__(self, client: CogniteClient):
        self._client = client

        self.bid_configuration_day_ahead = BidConfigurationDayAheadAPI(client)
        self.date_specification = DateSpecificationAPI(client)
        self.generator = GeneratorAPI(client)
        self.generator_efficiency_curve = GeneratorEfficiencyCurveAPI(client)
        self.market_configuration = MarketConfigurationAPI(client)
        self.partial_bid_configuration = PartialBidConfigurationAPI(client)
        self.plant = PlantAPI(client)
        self.plant_information = PlantInformationAPI(client)
        self.plant_water_value_based = PlantWaterValueBasedAPI(client)
        self.power_asset = PowerAssetAPI(client)
        self.price_area = PriceAreaAPI(client)
        self.price_area_afrr = PriceAreaAFRRAPI(client)
        self.price_area_day_ahead = PriceAreaDayAheadAPI(client)
        self.price_area_information = PriceAreaInformationAPI(client)
        self.turbine_efficiency_curve = TurbineEfficiencyCurveAPI(client)
        self.watercourse = WatercourseAPI(client)

    def graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> GraphQLList:
        """Execute a GraphQl query against the frontend_Asset data model.

            Args:
                query (str): The GraphQL query to issue.
                variables (dict[str, Any] | None): An optional dict of variables to pass to the query.
        """
        data_model_id = dm.DataModelId("power_ops_core", "frontend_Asset", "1")
        result = self._client.data_modeling.graphql.query(data_model_id, query, variables)
        return GraphQLQueryResponse(data_model_id).parse(result)


class DayAheadBidAPIs:
    """
    DayAheadBidAPIs

    Data Model:
        space: power_ops_core
        externalId: frontend_DayAheadBid
        version: 1

    """

    def __init__(self, client: CogniteClient):
        self._client = client

        self.alert = AlertAPI(client)
        self.bid_configuration_day_ahead = BidConfigurationDayAheadAPI(client)
        self.bid_document = BidDocumentAPI(client)
        self.bid_document_day_ahead = BidDocumentDayAheadAPI(client)
        self.bid_matrix = BidMatrixAPI(client)
        self.bid_matrix_information = BidMatrixInformationAPI(client)
        self.date_specification = DateSpecificationAPI(client)
        self.market_configuration = MarketConfigurationAPI(client)
        self.partial_bid_configuration = PartialBidConfigurationAPI(client)
        self.partial_bid_matrix_information = PartialBidMatrixInformationAPI(client)
        self.partial_bid_matrix_information_with_scenarios = PartialBidMatrixInformationWithScenariosAPI(client)
        self.power_asset = PowerAssetAPI(client)
        self.price_area = PriceAreaAPI(client)
        self.price_area_day_ahead = PriceAreaDayAheadAPI(client)
        self.price_production = PriceProductionAPI(client)
        self.shop_attribute_mapping = ShopAttributeMappingAPI(client)
        self.shop_case = ShopCaseAPI(client)
        self.shop_commands = ShopCommandsAPI(client)
        self.shop_file = ShopFileAPI(client)
        self.shop_model = ShopModelAPI(client)
        self.shop_output_time_series_definition = ShopOutputTimeSeriesDefinitionAPI(client)
        self.shop_penalty_report = ShopPenaltyReportAPI(client)
        self.shop_result = ShopResultAPI(client)
        self.shop_scenario = ShopScenarioAPI(client)
        self.shop_time_series = ShopTimeSeriesAPI(client)

    def graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> GraphQLList:
        """Execute a GraphQl query against the frontend_DayAheadBid data model.

            Args:
                query (str): The GraphQL query to issue.
                variables (dict[str, Any] | None): An optional dict of variables to pass to the query.
        """
        data_model_id = dm.DataModelId("power_ops_core", "frontend_DayAheadBid", "1")
        result = self._client.data_modeling.graphql.query(data_model_id, query, variables)
        return GraphQLQueryResponse(data_model_id).parse(result)


class PowerOpsModelsV1Client:
    """
    PowerOpsModelsV1Client

    Generated with:
        pygen = 0.99.28
        cognite-sdk = 7.54.12
        pydantic = 2.8.2

    """

    def __init__(self, config_or_client: CogniteClient | ClientConfig):
        if isinstance(config_or_client, CogniteClient):
            client = config_or_client
        elif isinstance(config_or_client, ClientConfig):
            client = CogniteClient(config_or_client)
        else:
            raise ValueError(f"Expected CogniteClient or ClientConfig, got {type(config_or_client)}")
        # The client name is used for aggregated logging of Pygen Usage
        client.config.client_name = "CognitePygen:0.99.28"

        self.benchmarking_day_ahead = BenchmarkingDayAheadAPIs(client)
        self.shop_based_day_ahead_bid_process = ShopBasedDayAheadBidProcesAPIs(client)
        self.total_bid_matrix_calculation = TotalBidMatrixCalculationAPIs(client)
        self.water_value_based_day_ahead_bid_process = WaterValueBasedDayAheadBidProcesAPIs(client)
        self.day_ahead_configuration = DayAheadConfigurationAPIs(client)
        self.afrr_bid = AFRRBidAPIs(client)
        self.power_asset = PowerAssetAPIs(client)
        self.day_ahead_bid = DayAheadBidAPIs(client)

        self._client = client


    def upsert(
        self,
        items: data_classes.DomainModelWrite | Sequence[data_classes.DomainModelWrite],
        replace: bool = False,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> data_classes.ResourcesWriteResult:
        """Add or update (upsert) items.

        Args:
            items: One or more instances of the pygen generated data classes.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method will, by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
            allow_version_increase (bool): If set to true, the version of the instance will be increased if the instance already exists.
                If you get an error: 'A version conflict caused the ingest to fail', you can set this to true to allow
                the version to increase.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        """
        instances = self._create_instances(items, write_none, allow_version_increase)
        result = self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )
        time_series = TimeSeriesList([])
        if instances.time_series:
            time_series = self._client.time_series.upsert(instances.time_series, mode="patch")

        return data_classes.ResourcesWriteResult(result.nodes, result.edges, TimeSeriesList(time_series))

    def _create_instances(
        self,
        items: data_classes.DomainModelWrite | Sequence[data_classes.DomainModelWrite],
        write_none: bool,
        allow_version_increase: bool,
    ) -> data_classes.ResourcesWrite:
        if isinstance(items, data_classes.DomainModelWrite):
            instances = items.to_instances_write(write_none, allow_version_increase)
        else:
            instances = data_classes.ResourcesWrite()
            cache: set[tuple[str, str]] = set()
            for item in items:
                instances.extend(
                    item._to_instances_write(
                        cache,
                        write_none,
                        allow_version_increase,
                    )
                )
        return instances

    def apply(
        self,
        items: data_classes.DomainModelWrite | Sequence[data_classes.DomainModelWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> data_classes.ResourcesWriteResult:
        """Add or update (upsert) items.

        Args:
            items: One or more instances of the pygen generated data classes.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method will, by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the instead."
            "The motivation is that .upsert is a more descriptive name for the operation.",
            UserWarning,
            stacklevel=2,
        )
        return self.upsert(items, replace, write_none)

    def delete(
        self,
        external_id: (
            str | SequenceNotStr[str] | data_classes.DomainModelWrite | Sequence[data_classes.DomainModelWrite]
        ),
        space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more items.

        If you pass in an item, it will be deleted recursively, i.e., all connected nodes and edges
        will be deleted as well.

        Args:
            external_id: The external id or items(s) to delete.
            space: The space where all the item(s) are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete item by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.delete("my_node_external_id")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        elif isinstance(external_id, Sequence) and all(isinstance(item, str) for item in external_id):
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id_) for id_ in external_id if isinstance(id_, str)],
            )
        elif isinstance(external_id, data_classes.DomainModelWrite) or (
            isinstance(external_id, Sequence)
            and not isinstance(external_id, str)
            and all(isinstance(item, data_classes.DomainModelWrite) for item in external_id)
        ):
            resources = self._create_instances(external_id, False, False)
            return self._client.data_modeling.instances.delete(
                nodes=resources.nodes.as_ids(),
                edges=resources.edges.as_ids(),
            )
        else:
            raise ValueError(
                "Expected str, list of str, or DomainModelWrite, list of DomainModelWrite," f"got {type(external_id)}"
            )

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> PowerOpsModelsV1Client:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> PowerOpsModelsV1Client:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)
