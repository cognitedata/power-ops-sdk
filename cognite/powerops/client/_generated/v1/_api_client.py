from __future__ import annotations

import warnings
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from cognite.client import ClientConfig, CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import FileMetadataList, SequenceList, TimeSeriesList

from cognite.powerops.client._generated.v1 import data_classes
from cognite.powerops.client._generated.v1._api import (    AlertAPI,    BenchmarkingCalculationInputAPI,    BenchmarkingCalculationOutputAPI,    BenchmarkingConfigurationDayAheadAPI,    BenchmarkingProductionObligationDayAheadAPI,    BenchmarkingResultDayAheadAPI,    BenchmarkingShopCaseAPI,    BenchmarkingTaskDispatcherInputDayAheadAPI,    BenchmarkingTaskDispatcherOutputDayAheadAPI,    BidConfigurationDayAheadAPI,    BidDocumentAPI,    BidDocumentAFRRAPI,    BidDocumentDayAheadAPI,    BidMatrixAPI,    BidMatrixInformationAPI,    BidRowAPI,    DateSpecificationAPI,    FunctionInputAPI,    FunctionOutputAPI,    GeneratorAPI,    GeneratorEfficiencyCurveAPI,    MarketConfigurationAPI,    MultiScenarioPartialBidMatrixCalculationInputAPI,    PartialBidConfigurationAPI,    PartialBidMatrixCalculationInputAPI,    PartialBidMatrixCalculationOutputAPI,    PartialBidMatrixInformationAPI,    PartialBidMatrixInformationWithScenariosAPI,    PlantAPI,    PlantInformationAPI,    PlantWaterValueBasedAPI,    PowerAssetAPI,    PriceAreaAPI,    PriceAreaAFRRAPI,    PriceAreaDayAheadAPI,    PriceAreaInformationAPI,    PriceProductionAPI,    ShopAttributeMappingAPI,    ShopBasedPartialBidConfigurationAPI,    ShopCaseAPI,    ShopCommandsAPI,    ShopFileAPI,    ShopModelAPI,    ShopModelWithAssetsAPI,    ShopOutputTimeSeriesDefinitionAPI,    ShopPenaltyReportAPI,    ShopPreprocessorInputAPI,    ShopPreprocessorOutputAPI,    ShopResultAPI,    ShopScenarioAPI,    ShopScenarioSetAPI,    ShopTimeResolutionAPI,    ShopTimeSeriesAPI,    ShopTriggerInputAPI,    ShopTriggerOutputAPI,    TaskDispatcherInputAPI,    TaskDispatcherOutputAPI,    TotalBidMatrixCalculationInputAPI,    TotalBidMatrixCalculationOutputAPI,    TurbineEfficiencyCurveAPI,    WaterValueBasedPartialBidConfigurationAPI,    WaterValueBasedPartialBidMatrixCalculationInputAPI,    WatercourseAPI,)
from cognite.powerops.client._generated.v1._api._core import GraphQLQueryResponse, SequenceNotStr
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE, GraphQLList

class BenchmarkingDayAheadAPIs:
    """
    BenchmarkingDayAheadAPIs

    Data Model:
        space: power_ops_core
        externalId: compute_BenchmarkingDayAhead
        version: 1

    """
    _data_model_id = dm.DataModelId("power_ops_core", "compute_ShopBasedDayAhead", "1")

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
        self.shop_time_resolution = ShopTimeResolutionAPI(client)
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
    _data_model_id = dm.DataModelId("power_ops_core", "compute_ShopBasedDayAhead", "1")

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
        self.shop_time_resolution = ShopTimeResolutionAPI(client)
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
    _data_model_id = dm.DataModelId("power_ops_core", "compute_ShopBasedDayAhead", "1")

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
        self.shop_time_resolution = ShopTimeResolutionAPI(client)
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
    _data_model_id = dm.DataModelId("power_ops_core", "compute_ShopBasedDayAhead", "1")

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
    _data_model_id = dm.DataModelId("power_ops_core", "compute_ShopBasedDayAhead", "1")

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
        self.shop_time_resolution = ShopTimeResolutionAPI(client)
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
    _data_model_id = dm.DataModelId("power_ops_core", "compute_ShopBasedDayAhead", "1")

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
    _data_model_id = dm.DataModelId("power_ops_core", "compute_ShopBasedDayAhead", "1")

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
    _data_model_id = dm.DataModelId("power_ops_core", "compute_ShopBasedDayAhead", "1")

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
        self.shop_time_resolution = ShopTimeResolutionAPI(client)
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
        pygen = 1.2.12
        cognite-sdk = 7.76.0
        pydantic = 2.11.7

    """

    def __init__(self, config_or_client: CogniteClient | ClientConfig):
        if isinstance(config_or_client, CogniteClient):
            client = config_or_client
        elif isinstance(config_or_client, ClientConfig):
            client = CogniteClient(config_or_client)
        else:
            raise ValueError(f"Expected CogniteClient or ClientConfig, got {type(config_or_client)}")
        # The client name is used for aggregated logging of Pygen Usage
        if not client.config.client_name.startswith("CognitePygen"):
            client.config.client_name = f"CognitePygen:1.2.12:SDK:{client.config.client_name}"

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
        allow_version_increase: bool = False,
    ) -> data_classes.ResourcesWriteResult:
        """Add or update (upsert) items.

        This method will create the nodes, edges, timeseries, files and sequences of the supplied items.

        Args:
            items: One or more instances of the pygen generated data classes.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and
                existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)?
                Note: This setting applies for all nodes or edges specified in the ingestion call.
            allow_version_increase (bool): If set to true, the version of the instance will be increased
                if the instance already exists.
                If you get an error: 'A version conflict caused the ingest to fail', you can set this to true to allow
                the version to increase.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        """
        instances = self._create_instances(items, allow_version_increase)
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
        files = FileMetadataList([])
        if instances.files:
            for file in instances.files:
                created, _ = self._client.files.create(file, overwrite=True)
                files.append(created)

        sequences = SequenceList([])
        if instances.sequences:
            sequences = self._client.sequences.upsert(instances.sequences, mode="patch")

        return data_classes.ResourcesWriteResult(result.nodes, result.edges, time_series, files, sequences)

    def _create_instances(
        self,
        items: data_classes.DomainModelWrite | Sequence[data_classes.DomainModelWrite],
        allow_version_increase: bool,
    ) -> data_classes.ResourcesWrite:
        if isinstance(items, data_classes.DomainModelWrite):
            instances = items.to_instances_write(allow_version_increase)
        else:
            instances = data_classes.ResourcesWrite()
            cache: set[tuple[str, str]] = set()
            for item in items:
                instances.extend(
                    item._to_resources_write(
                        cache,
                        allow_version_increase,
                    )
                )
        return instances

    def delete(
        self,
        external_id: (
            str | dm.NodeId | data_classes.DomainModelWrite | SequenceNotStr[str | dm.NodeId | data_classes.DomainModelWrite]
        ),
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> dm.InstancesDeleteResult:
        """Delete one or more items.

        If you pass in an item, it will be deleted recursively, i.e., all connected nodes and edges
        will be deleted as well.

        Args:
            external_id: The external id or items(s) to delete. Can also be a list of NodeId(s) or DomainModelWrite(s).
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
        elif isinstance(external_id, dm.NodeId):
            return self._client.data_modeling.instances.delete(nodes=external_id)
        elif isinstance(external_id, data_classes.DomainModelWrite):
            resources = self._create_instances(external_id, False)
            return self._client.data_modeling.instances.delete(
                nodes=resources.nodes.as_ids(),
                edges=resources.edges.as_ids(),
            )
        elif isinstance(external_id, Sequence):
            node_ids: list[dm.NodeId] = []
            edge_ids: list[dm.EdgeId] = []
            for item in external_id:
                if isinstance(item, str):
                    node_ids.append(dm.NodeId(space, item))
                elif isinstance(item, dm.NodeId):
                    node_ids.append(item)
                elif isinstance(item, data_classes.DomainModelWrite):
                    resources = self._create_instances(item, False)
                    node_ids.extend(resources.nodes.as_ids())
                    edge_ids.extend(resources.edges.as_ids())
                else:
                    raise ValueError(
                        f"Expected str, NodeId, or DomainModelWrite, Sequence of these types. Got {type(external_id)}"
                    )
            return self._client.data_modeling.instances.delete(nodes=node_ids, edges=edge_ids)
        else:
            raise ValueError(
                f"Expected str, NodeId, or DomainModelWrite, Sequence of these types. Got {type(external_id)}"
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
