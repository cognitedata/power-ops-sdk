from __future__ import annotations

import warnings
from pathlib import Path
from typing import Sequence

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.data_classes import TimeSeriesList
from cognite.client.credentials import OAuthClientCredentials

from ._api.alert import AlertAPI
from ._api.bid_configuration import BidConfigurationAPI
from ._api.bid_document import BidDocumentAPI
from ._api.bid_document_afrr import BidDocumentAFRRAPI
from ._api.bid_document_day_ahead import BidDocumentDayAheadAPI
from ._api.bid_matrix import BidMatrixAPI
from ._api.bid_row import BidRowAPI
from ._api.case import CaseAPI
from ._api.commands import CommandsAPI
from ._api.function_input import FunctionInputAPI
from ._api.function_output import FunctionOutputAPI
from ._api.generator import GeneratorAPI
from ._api.generator_efficiency_curve import GeneratorEfficiencyCurveAPI
from ._api.mapping import MappingAPI
from ._api.market_configuration import MarketConfigurationAPI
from ._api.model_template import ModelTemplateAPI
from ._api.partial_bid_configuration import PartialBidConfigurationAPI
from ._api.partial_bid_matrix_calculation_input import PartialBidMatrixCalculationInputAPI
from ._api.partial_bid_matrix_calculation_output import PartialBidMatrixCalculationOutputAPI
from ._api.plant import PlantAPI
from ._api.power_asset import PowerAssetAPI
from ._api.preprocessor_input import PreprocessorInputAPI
from ._api.preprocessor_output import PreprocessorOutputAPI
from ._api.price_area import PriceAreaAPI
from ._api.price_area_afrr import PriceAreaAFRRAPI
from ._api.price_production import PriceProductionAPI
from ._api.shop_result import SHOPResultAPI
from ._api.shop_time_series import SHOPTimeSeriesAPI
from ._api.shop_trigger_input import SHOPTriggerInputAPI
from ._api.shop_trigger_output import SHOPTriggerOutputAPI
from ._api.scenario import ScenarioAPI
from ._api.scenario_set import ScenarioSetAPI
from ._api.shop_based_partial_bid_configuration import ShopBasedPartialBidConfigurationAPI
from ._api.shop_partial_bid_matrix_calculation_input import ShopPartialBidMatrixCalculationInputAPI
from ._api.task_dispatcher_input import TaskDispatcherInputAPI
from ._api.task_dispatcher_output import TaskDispatcherOutputAPI
from ._api.total_bid_matrix_calculation_input import TotalBidMatrixCalculationInputAPI
from ._api.total_bid_matrix_calculation_output import TotalBidMatrixCalculationOutputAPI
from ._api.turbine_efficiency_curve import TurbineEfficiencyCurveAPI
from ._api.water_value_based_partial_bid_configuration import WaterValueBasedPartialBidConfigurationAPI
from ._api.water_value_based_partial_bid_matrix_calculation_input import (
    WaterValueBasedPartialBidMatrixCalculationInputAPI,
)
from ._api._core import SequenceNotStr, GraphQLQueryResponse
from .data_classes._core import DEFAULT_INSTANCE_SPACE, GraphQLList
from . import data_classes


class SHOPBasedDayAheadBidProcesAPIs:
    """
    SHOPBasedDayAheadBidProcesAPIs

    Data Model:
        space: sp_powerops_models_temp
        externalId: compute_SHOPBasedDayAhead
        version: 1

    """

    def __init__(self, client: CogniteClient):
        view_by_read_class = {
            data_classes.Alert: dm.ViewId("sp_powerops_models_temp", "Alert", "1"),
            data_classes.BidConfiguration: dm.ViewId("sp_powerops_models_temp", "BidConfiguration", "1"),
            data_classes.BidMatrix: dm.ViewId("sp_powerops_models_temp", "BidMatrix", "1"),
            data_classes.Case: dm.ViewId("sp_powerops_models_temp", "Case", "1"),
            data_classes.Commands: dm.ViewId("sp_powerops_models_temp", "Commands", "1"),
            data_classes.FunctionInput: dm.ViewId("sp_powerops_models_temp", "FunctionInput", "1"),
            data_classes.FunctionOutput: dm.ViewId("sp_powerops_models_temp", "FunctionOutput", "1"),
            data_classes.Mapping: dm.ViewId("sp_powerops_models_temp", "Mapping", "1"),
            data_classes.MarketConfiguration: dm.ViewId("sp_powerops_models_temp", "MarketConfiguration", "1"),
            data_classes.ModelTemplate: dm.ViewId("sp_powerops_models_temp", "ModelTemplate", "1"),
            data_classes.PartialBidConfiguration: dm.ViewId("sp_powerops_models_temp", "PartialBidConfiguration", "1"),
            data_classes.PartialBidMatrixCalculationInput: dm.ViewId(
                "sp_powerops_models_temp", "PartialBidMatrixCalculationInput", "1"
            ),
            data_classes.PartialBidMatrixCalculationOutput: dm.ViewId(
                "sp_powerops_models_temp", "PartialBidMatrixCalculationOutput", "1"
            ),
            data_classes.PowerAsset: dm.ViewId("sp_powerops_models_temp", "PowerAsset", "1"),
            data_classes.PreprocessorInput: dm.ViewId("sp_powerops_models_temp", "PreprocessorInput", "1"),
            data_classes.PreprocessorOutput: dm.ViewId("sp_powerops_models_temp", "PreprocessorOutput", "1"),
            data_classes.PriceArea: dm.ViewId("sp_powerops_models_temp", "PriceArea", "1"),
            data_classes.PriceProduction: dm.ViewId("sp_powerops_models_temp", "PriceProduction", "1"),
            data_classes.SHOPResult: dm.ViewId("sp_powerops_models_temp", "SHOPResult", "1"),
            data_classes.SHOPTimeSeries: dm.ViewId("sp_powerops_models_temp", "SHOPTimeSeries", "1"),
            data_classes.SHOPTriggerInput: dm.ViewId("sp_powerops_models_temp", "SHOPTriggerInput", "1"),
            data_classes.SHOPTriggerOutput: dm.ViewId("sp_powerops_models_temp", "SHOPTriggerOutput", "1"),
            data_classes.Scenario: dm.ViewId("sp_powerops_models_temp", "Scenario", "1"),
            data_classes.ScenarioSet: dm.ViewId("sp_powerops_models_temp", "ScenarioSet", "1"),
            data_classes.ShopBasedPartialBidConfiguration: dm.ViewId(
                "sp_powerops_models_temp", "ShopBasedPartialBidConfiguration", "1"
            ),
            data_classes.ShopPartialBidMatrixCalculationInput: dm.ViewId(
                "sp_powerops_models_temp", "ShopPartialBidMatrixCalculationInput", "1"
            ),
            data_classes.TaskDispatcherInput: dm.ViewId("sp_powerops_models_temp", "TaskDispatcherInput", "1"),
            data_classes.TaskDispatcherOutput: dm.ViewId("sp_powerops_models_temp", "TaskDispatcherOutput", "1"),
        }
        self._view_by_read_class = view_by_read_class
        self._client = client

        self.alert = AlertAPI(client, view_by_read_class)
        self.bid_configuration = BidConfigurationAPI(client, view_by_read_class)
        self.bid_matrix = BidMatrixAPI(client, view_by_read_class)
        self.case = CaseAPI(client, view_by_read_class)
        self.commands = CommandsAPI(client, view_by_read_class)
        self.function_input = FunctionInputAPI(client, view_by_read_class)
        self.function_output = FunctionOutputAPI(client, view_by_read_class)
        self.mapping = MappingAPI(client, view_by_read_class)
        self.market_configuration = MarketConfigurationAPI(client, view_by_read_class)
        self.model_template = ModelTemplateAPI(client, view_by_read_class)
        self.partial_bid_configuration = PartialBidConfigurationAPI(client, view_by_read_class)
        self.partial_bid_matrix_calculation_input = PartialBidMatrixCalculationInputAPI(client, view_by_read_class)
        self.partial_bid_matrix_calculation_output = PartialBidMatrixCalculationOutputAPI(client, view_by_read_class)
        self.power_asset = PowerAssetAPI(client, view_by_read_class)
        self.preprocessor_input = PreprocessorInputAPI(client, view_by_read_class)
        self.preprocessor_output = PreprocessorOutputAPI(client, view_by_read_class)
        self.price_area = PriceAreaAPI(client, view_by_read_class)
        self.price_production = PriceProductionAPI(client, view_by_read_class)
        self.shop_result = SHOPResultAPI(client, view_by_read_class)
        self.shop_time_series = SHOPTimeSeriesAPI(client, view_by_read_class)
        self.shop_trigger_input = SHOPTriggerInputAPI(client, view_by_read_class)
        self.shop_trigger_output = SHOPTriggerOutputAPI(client, view_by_read_class)
        self.scenario = ScenarioAPI(client, view_by_read_class)
        self.scenario_set = ScenarioSetAPI(client, view_by_read_class)
        self.shop_based_partial_bid_configuration = ShopBasedPartialBidConfigurationAPI(client, view_by_read_class)
        self.shop_partial_bid_matrix_calculation_input = ShopPartialBidMatrixCalculationInputAPI(
            client, view_by_read_class
        )
        self.task_dispatcher_input = TaskDispatcherInputAPI(client, view_by_read_class)
        self.task_dispatcher_output = TaskDispatcherOutputAPI(client, view_by_read_class)

    def graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> GraphQLList:
        """Execute a GraphQl query against the compute_SHOPBasedDayAhead data model.

        Args:
            query (str): The GraphQL query to issue.
            variables (dict[str, Any] | None): An optional dict of variables to pass to the query.
        """
        data_model_id = dm.DataModelId("sp_powerops_models_temp", "compute_SHOPBasedDayAhead", "1")
        result = self._client.data_modeling.graphql.query(data_model_id, query, variables)
        return GraphQLQueryResponse(data_model_id).parse(result)


class TotalBidMatrixCalculationAPIs:
    """
    TotalBidMatrixCalculationAPIs

    Data Model:
        space: sp_powerops_models_temp
        externalId: compute_TotalBidMatrixCalculation
        version: 1

    """

    def __init__(self, client: CogniteClient):
        view_by_read_class = {
            data_classes.Alert: dm.ViewId("sp_powerops_models_temp", "Alert", "1"),
            data_classes.BidConfiguration: dm.ViewId("sp_powerops_models_temp", "BidConfiguration", "1"),
            data_classes.BidDocument: dm.ViewId("sp_powerops_models_temp", "BidDocument", "1"),
            data_classes.BidDocumentDayAhead: dm.ViewId("sp_powerops_models_temp", "BidDocumentDayAhead", "1"),
            data_classes.BidMatrix: dm.ViewId("sp_powerops_models_temp", "BidMatrix", "1"),
            data_classes.Case: dm.ViewId("sp_powerops_models_temp", "Case", "1"),
            data_classes.Commands: dm.ViewId("sp_powerops_models_temp", "Commands", "1"),
            data_classes.FunctionInput: dm.ViewId("sp_powerops_models_temp", "FunctionInput", "1"),
            data_classes.FunctionOutput: dm.ViewId("sp_powerops_models_temp", "FunctionOutput", "1"),
            data_classes.Mapping: dm.ViewId("sp_powerops_models_temp", "Mapping", "1"),
            data_classes.MarketConfiguration: dm.ViewId("sp_powerops_models_temp", "MarketConfiguration", "1"),
            data_classes.ModelTemplate: dm.ViewId("sp_powerops_models_temp", "ModelTemplate", "1"),
            data_classes.PartialBidConfiguration: dm.ViewId("sp_powerops_models_temp", "PartialBidConfiguration", "1"),
            data_classes.PowerAsset: dm.ViewId("sp_powerops_models_temp", "PowerAsset", "1"),
            data_classes.PriceArea: dm.ViewId("sp_powerops_models_temp", "PriceArea", "1"),
            data_classes.PriceProduction: dm.ViewId("sp_powerops_models_temp", "PriceProduction", "1"),
            data_classes.SHOPResult: dm.ViewId("sp_powerops_models_temp", "SHOPResult", "1"),
            data_classes.SHOPTimeSeries: dm.ViewId("sp_powerops_models_temp", "SHOPTimeSeries", "1"),
            data_classes.Scenario: dm.ViewId("sp_powerops_models_temp", "Scenario", "1"),
            data_classes.TotalBidMatrixCalculationInput: dm.ViewId(
                "sp_powerops_models_temp", "TotalBidMatrixCalculationInput", "1"
            ),
            data_classes.TotalBidMatrixCalculationOutput: dm.ViewId(
                "sp_powerops_models_temp", "TotalBidMatrixCalculationOutput", "1"
            ),
        }
        self._view_by_read_class = view_by_read_class
        self._client = client

        self.alert = AlertAPI(client, view_by_read_class)
        self.bid_configuration = BidConfigurationAPI(client, view_by_read_class)
        self.bid_document = BidDocumentAPI(client, view_by_read_class)
        self.bid_document_day_ahead = BidDocumentDayAheadAPI(client, view_by_read_class)
        self.bid_matrix = BidMatrixAPI(client, view_by_read_class)
        self.case = CaseAPI(client, view_by_read_class)
        self.commands = CommandsAPI(client, view_by_read_class)
        self.function_input = FunctionInputAPI(client, view_by_read_class)
        self.function_output = FunctionOutputAPI(client, view_by_read_class)
        self.mapping = MappingAPI(client, view_by_read_class)
        self.market_configuration = MarketConfigurationAPI(client, view_by_read_class)
        self.model_template = ModelTemplateAPI(client, view_by_read_class)
        self.partial_bid_configuration = PartialBidConfigurationAPI(client, view_by_read_class)
        self.power_asset = PowerAssetAPI(client, view_by_read_class)
        self.price_area = PriceAreaAPI(client, view_by_read_class)
        self.price_production = PriceProductionAPI(client, view_by_read_class)
        self.shop_result = SHOPResultAPI(client, view_by_read_class)
        self.shop_time_series = SHOPTimeSeriesAPI(client, view_by_read_class)
        self.scenario = ScenarioAPI(client, view_by_read_class)
        self.total_bid_matrix_calculation_input = TotalBidMatrixCalculationInputAPI(client, view_by_read_class)
        self.total_bid_matrix_calculation_output = TotalBidMatrixCalculationOutputAPI(client, view_by_read_class)

    def graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> GraphQLList:
        """Execute a GraphQl query against the compute_TotalBidMatrixCalculation data model.

        Args:
            query (str): The GraphQL query to issue.
            variables (dict[str, Any] | None): An optional dict of variables to pass to the query.
        """
        data_model_id = dm.DataModelId("sp_powerops_models_temp", "compute_TotalBidMatrixCalculation", "1")
        result = self._client.data_modeling.graphql.query(data_model_id, query, variables)
        return GraphQLQueryResponse(data_model_id).parse(result)


class WaterValueBasedDayAheadBidProcesAPIs:
    """
    WaterValueBasedDayAheadBidProcesAPIs

    Data Model:
        space: sp_powerops_models_temp
        externalId: compute_WaterValueBasedDayAheadBid
        version: 1

    """

    def __init__(self, client: CogniteClient):
        view_by_read_class = {
            data_classes.Alert: dm.ViewId("sp_powerops_models_temp", "Alert", "1"),
            data_classes.BidConfiguration: dm.ViewId("sp_powerops_models_temp", "BidConfiguration", "1"),
            data_classes.BidMatrix: dm.ViewId("sp_powerops_models_temp", "BidMatrix", "1"),
            data_classes.FunctionInput: dm.ViewId("sp_powerops_models_temp", "FunctionInput", "1"),
            data_classes.FunctionOutput: dm.ViewId("sp_powerops_models_temp", "FunctionOutput", "1"),
            data_classes.Generator: dm.ViewId("sp_powerops_models_temp", "Generator", "1"),
            data_classes.GeneratorEfficiencyCurve: dm.ViewId(
                "sp_powerops_models_temp", "GeneratorEfficiencyCurve", "1"
            ),
            data_classes.MarketConfiguration: dm.ViewId("sp_powerops_models_temp", "MarketConfiguration", "1"),
            data_classes.PartialBidConfiguration: dm.ViewId("sp_powerops_models_temp", "PartialBidConfiguration", "1"),
            data_classes.PartialBidMatrixCalculationInput: dm.ViewId(
                "sp_powerops_models_temp", "PartialBidMatrixCalculationInput", "1"
            ),
            data_classes.PartialBidMatrixCalculationOutput: dm.ViewId(
                "sp_powerops_models_temp", "PartialBidMatrixCalculationOutput", "1"
            ),
            data_classes.Plant: dm.ViewId("sp_powerops_models_temp", "Plant", "1"),
            data_classes.PowerAsset: dm.ViewId("sp_powerops_models_temp", "PowerAsset", "1"),
            data_classes.PriceArea: dm.ViewId("sp_powerops_models_temp", "PriceArea", "1"),
            data_classes.TaskDispatcherInput: dm.ViewId("sp_powerops_models_temp", "TaskDispatcherInput", "1"),
            data_classes.TaskDispatcherOutput: dm.ViewId("sp_powerops_models_temp", "TaskDispatcherOutput", "1"),
            data_classes.TurbineEfficiencyCurve: dm.ViewId("sp_powerops_models_temp", "TurbineEfficiencyCurve", "1"),
            data_classes.WaterValueBasedPartialBidConfiguration: dm.ViewId(
                "sp_powerops_models_temp", "WaterValueBasedPartialBidConfiguration", "1"
            ),
            data_classes.WaterValueBasedPartialBidMatrixCalculationInput: dm.ViewId(
                "sp_powerops_models_temp", "WaterValueBasedPartialBidMatrixCalculationInput", "1"
            ),
        }
        self._view_by_read_class = view_by_read_class
        self._client = client

        self.alert = AlertAPI(client, view_by_read_class)
        self.bid_configuration = BidConfigurationAPI(client, view_by_read_class)
        self.bid_matrix = BidMatrixAPI(client, view_by_read_class)
        self.function_input = FunctionInputAPI(client, view_by_read_class)
        self.function_output = FunctionOutputAPI(client, view_by_read_class)
        self.generator = GeneratorAPI(client, view_by_read_class)
        self.generator_efficiency_curve = GeneratorEfficiencyCurveAPI(client, view_by_read_class)
        self.market_configuration = MarketConfigurationAPI(client, view_by_read_class)
        self.partial_bid_configuration = PartialBidConfigurationAPI(client, view_by_read_class)
        self.partial_bid_matrix_calculation_input = PartialBidMatrixCalculationInputAPI(client, view_by_read_class)
        self.partial_bid_matrix_calculation_output = PartialBidMatrixCalculationOutputAPI(client, view_by_read_class)
        self.plant = PlantAPI(client, view_by_read_class)
        self.power_asset = PowerAssetAPI(client, view_by_read_class)
        self.price_area = PriceAreaAPI(client, view_by_read_class)
        self.task_dispatcher_input = TaskDispatcherInputAPI(client, view_by_read_class)
        self.task_dispatcher_output = TaskDispatcherOutputAPI(client, view_by_read_class)
        self.turbine_efficiency_curve = TurbineEfficiencyCurveAPI(client, view_by_read_class)
        self.water_value_based_partial_bid_configuration = WaterValueBasedPartialBidConfigurationAPI(
            client, view_by_read_class
        )
        self.water_value_based_partial_bid_matrix_calculation_input = (
            WaterValueBasedPartialBidMatrixCalculationInputAPI(client, view_by_read_class)
        )

    def graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> GraphQLList:
        """Execute a GraphQl query against the compute_WaterValueBasedDayAheadBid data model.

        Args:
            query (str): The GraphQL query to issue.
            variables (dict[str, Any] | None): An optional dict of variables to pass to the query.
        """
        data_model_id = dm.DataModelId("sp_powerops_models_temp", "compute_WaterValueBasedDayAheadBid", "1")
        result = self._client.data_modeling.graphql.query(data_model_id, query, variables)
        return GraphQLQueryResponse(data_model_id).parse(result)


class DayAheadConfigurationAPIs:
    """
    DayAheadConfigurationAPIs

    Data Model:
        space: sp_powerops_models_temp
        externalId: config_DayAheadConfiguration
        version: 1

    """

    def __init__(self, client: CogniteClient):
        view_by_read_class = {
            data_classes.BidConfiguration: dm.ViewId("sp_powerops_models_temp", "BidConfiguration", "1"),
            data_classes.Commands: dm.ViewId("sp_powerops_models_temp", "Commands", "1"),
            data_classes.Generator: dm.ViewId("sp_powerops_models_temp", "Generator", "1"),
            data_classes.GeneratorEfficiencyCurve: dm.ViewId(
                "sp_powerops_models_temp", "GeneratorEfficiencyCurve", "1"
            ),
            data_classes.Mapping: dm.ViewId("sp_powerops_models_temp", "Mapping", "1"),
            data_classes.MarketConfiguration: dm.ViewId("sp_powerops_models_temp", "MarketConfiguration", "1"),
            data_classes.ModelTemplate: dm.ViewId("sp_powerops_models_temp", "ModelTemplate", "1"),
            data_classes.PartialBidConfiguration: dm.ViewId("sp_powerops_models_temp", "PartialBidConfiguration", "1"),
            data_classes.Plant: dm.ViewId("sp_powerops_models_temp", "Plant", "1"),
            data_classes.PowerAsset: dm.ViewId("sp_powerops_models_temp", "PowerAsset", "1"),
            data_classes.PriceArea: dm.ViewId("sp_powerops_models_temp", "PriceArea", "1"),
            data_classes.Scenario: dm.ViewId("sp_powerops_models_temp", "Scenario", "1"),
            data_classes.ScenarioSet: dm.ViewId("sp_powerops_models_temp", "ScenarioSet", "1"),
            data_classes.ShopBasedPartialBidConfiguration: dm.ViewId(
                "sp_powerops_models_temp", "ShopBasedPartialBidConfiguration", "1"
            ),
            data_classes.TurbineEfficiencyCurve: dm.ViewId("sp_powerops_models_temp", "TurbineEfficiencyCurve", "1"),
            data_classes.WaterValueBasedPartialBidConfiguration: dm.ViewId(
                "sp_powerops_models_temp", "WaterValueBasedPartialBidConfiguration", "1"
            ),
        }
        self._view_by_read_class = view_by_read_class
        self._client = client

        self.bid_configuration = BidConfigurationAPI(client, view_by_read_class)
        self.commands = CommandsAPI(client, view_by_read_class)
        self.generator = GeneratorAPI(client, view_by_read_class)
        self.generator_efficiency_curve = GeneratorEfficiencyCurveAPI(client, view_by_read_class)
        self.mapping = MappingAPI(client, view_by_read_class)
        self.market_configuration = MarketConfigurationAPI(client, view_by_read_class)
        self.model_template = ModelTemplateAPI(client, view_by_read_class)
        self.partial_bid_configuration = PartialBidConfigurationAPI(client, view_by_read_class)
        self.plant = PlantAPI(client, view_by_read_class)
        self.power_asset = PowerAssetAPI(client, view_by_read_class)
        self.price_area = PriceAreaAPI(client, view_by_read_class)
        self.scenario = ScenarioAPI(client, view_by_read_class)
        self.scenario_set = ScenarioSetAPI(client, view_by_read_class)
        self.shop_based_partial_bid_configuration = ShopBasedPartialBidConfigurationAPI(client, view_by_read_class)
        self.turbine_efficiency_curve = TurbineEfficiencyCurveAPI(client, view_by_read_class)
        self.water_value_based_partial_bid_configuration = WaterValueBasedPartialBidConfigurationAPI(
            client, view_by_read_class
        )

    def graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> GraphQLList:
        """Execute a GraphQl query against the config_DayAheadConfiguration data model.

        Args:
            query (str): The GraphQL query to issue.
            variables (dict[str, Any] | None): An optional dict of variables to pass to the query.
        """
        data_model_id = dm.DataModelId("sp_powerops_models_temp", "config_DayAheadConfiguration", "1")
        result = self._client.data_modeling.graphql.query(data_model_id, query, variables)
        return GraphQLQueryResponse(data_model_id).parse(result)


class AFRRBidAPIs:
    """
    AFRRBidAPIs

    Data Model:
        space: sp_powerops_models_temp
        externalId: frontend_AFRRBid
        version: 1

    """

    def __init__(self, client: CogniteClient):
        view_by_read_class = {
            data_classes.Alert: dm.ViewId("sp_powerops_models_temp", "Alert", "1"),
            data_classes.BidDocument: dm.ViewId("sp_powerops_models_temp", "BidDocument", "1"),
            data_classes.BidDocumentAFRR: dm.ViewId("sp_powerops_models_temp", "BidDocumentAFRR", "1"),
            data_classes.BidRow: dm.ViewId("sp_powerops_models_temp", "BidRow", "1"),
            data_classes.PowerAsset: dm.ViewId("sp_powerops_models_temp", "PowerAsset", "1"),
            data_classes.PriceArea: dm.ViewId("sp_powerops_models_temp", "PriceArea", "1"),
            data_classes.PriceAreaAFRR: dm.ViewId("sp_powerops_models_temp", "PriceAreaAFRR", "1"),
        }
        self._view_by_read_class = view_by_read_class
        self._client = client

        self.alert = AlertAPI(client, view_by_read_class)
        self.bid_document = BidDocumentAPI(client, view_by_read_class)
        self.bid_document_afrr = BidDocumentAFRRAPI(client, view_by_read_class)
        self.bid_row = BidRowAPI(client, view_by_read_class)
        self.power_asset = PowerAssetAPI(client, view_by_read_class)
        self.price_area = PriceAreaAPI(client, view_by_read_class)
        self.price_area_afrr = PriceAreaAFRRAPI(client, view_by_read_class)

    def graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> GraphQLList:
        """Execute a GraphQl query against the frontend_AFRRBid data model.

        Args:
            query (str): The GraphQL query to issue.
            variables (dict[str, Any] | None): An optional dict of variables to pass to the query.
        """
        data_model_id = dm.DataModelId("sp_powerops_models_temp", "frontend_AFRRBid", "1")
        result = self._client.data_modeling.graphql.query(data_model_id, query, variables)
        return GraphQLQueryResponse(data_model_id).parse(result)


class PowerAssetAPIs:
    """
    PowerAssetAPIs

    Data Model:
        space: sp_powerops_models_temp
        externalId: frontend_Asset
        version: 1

    """

    def __init__(self, client: CogniteClient):
        view_by_read_class = {
            data_classes.Generator: dm.ViewId("sp_powerops_models_temp", "Generator", "1"),
            data_classes.GeneratorEfficiencyCurve: dm.ViewId(
                "sp_powerops_models_temp", "GeneratorEfficiencyCurve", "1"
            ),
            data_classes.Plant: dm.ViewId("sp_powerops_models_temp", "Plant", "1"),
            data_classes.PowerAsset: dm.ViewId("sp_powerops_models_temp", "PowerAsset", "1"),
            data_classes.PriceArea: dm.ViewId("sp_powerops_models_temp", "PriceArea", "1"),
            data_classes.TurbineEfficiencyCurve: dm.ViewId("sp_powerops_models_temp", "TurbineEfficiencyCurve", "1"),
        }
        self._view_by_read_class = view_by_read_class
        self._client = client

        self.generator = GeneratorAPI(client, view_by_read_class)
        self.generator_efficiency_curve = GeneratorEfficiencyCurveAPI(client, view_by_read_class)
        self.plant = PlantAPI(client, view_by_read_class)
        self.power_asset = PowerAssetAPI(client, view_by_read_class)
        self.price_area = PriceAreaAPI(client, view_by_read_class)
        self.turbine_efficiency_curve = TurbineEfficiencyCurveAPI(client, view_by_read_class)

    def graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> GraphQLList:
        """Execute a GraphQl query against the frontend_Asset data model.

        Args:
            query (str): The GraphQL query to issue.
            variables (dict[str, Any] | None): An optional dict of variables to pass to the query.
        """
        data_model_id = dm.DataModelId("sp_powerops_models_temp", "frontend_Asset", "1")
        result = self._client.data_modeling.graphql.query(data_model_id, query, variables)
        return GraphQLQueryResponse(data_model_id).parse(result)


class DayAheadBidAPIs:
    """
    DayAheadBidAPIs

    Data Model:
        space: sp_powerops_models_temp
        externalId: frontend_DayAheadBid
        version: 1

    """

    def __init__(self, client: CogniteClient):
        view_by_read_class = {
            data_classes.Alert: dm.ViewId("sp_powerops_models_temp", "Alert", "1"),
            data_classes.BidConfiguration: dm.ViewId("sp_powerops_models_temp", "BidConfiguration", "1"),
            data_classes.BidDocument: dm.ViewId("sp_powerops_models_temp", "BidDocument", "1"),
            data_classes.BidDocumentDayAhead: dm.ViewId("sp_powerops_models_temp", "BidDocumentDayAhead", "1"),
            data_classes.BidMatrix: dm.ViewId("sp_powerops_models_temp", "BidMatrix", "1"),
            data_classes.Case: dm.ViewId("sp_powerops_models_temp", "Case", "1"),
            data_classes.Commands: dm.ViewId("sp_powerops_models_temp", "Commands", "1"),
            data_classes.Mapping: dm.ViewId("sp_powerops_models_temp", "Mapping", "1"),
            data_classes.MarketConfiguration: dm.ViewId("sp_powerops_models_temp", "MarketConfiguration", "1"),
            data_classes.ModelTemplate: dm.ViewId("sp_powerops_models_temp", "ModelTemplate", "1"),
            data_classes.PartialBidConfiguration: dm.ViewId("sp_powerops_models_temp", "PartialBidConfiguration", "1"),
            data_classes.PowerAsset: dm.ViewId("sp_powerops_models_temp", "PowerAsset", "1"),
            data_classes.PriceArea: dm.ViewId("sp_powerops_models_temp", "PriceArea", "1"),
            data_classes.PriceProduction: dm.ViewId("sp_powerops_models_temp", "PriceProduction", "1"),
            data_classes.SHOPResult: dm.ViewId("sp_powerops_models_temp", "SHOPResult", "1"),
            data_classes.SHOPTimeSeries: dm.ViewId("sp_powerops_models_temp", "SHOPTimeSeries", "1"),
            data_classes.Scenario: dm.ViewId("sp_powerops_models_temp", "Scenario", "1"),
        }
        self._view_by_read_class = view_by_read_class
        self._client = client

        self.alert = AlertAPI(client, view_by_read_class)
        self.bid_configuration = BidConfigurationAPI(client, view_by_read_class)
        self.bid_document = BidDocumentAPI(client, view_by_read_class)
        self.bid_document_day_ahead = BidDocumentDayAheadAPI(client, view_by_read_class)
        self.bid_matrix = BidMatrixAPI(client, view_by_read_class)
        self.case = CaseAPI(client, view_by_read_class)
        self.commands = CommandsAPI(client, view_by_read_class)
        self.mapping = MappingAPI(client, view_by_read_class)
        self.market_configuration = MarketConfigurationAPI(client, view_by_read_class)
        self.model_template = ModelTemplateAPI(client, view_by_read_class)
        self.partial_bid_configuration = PartialBidConfigurationAPI(client, view_by_read_class)
        self.power_asset = PowerAssetAPI(client, view_by_read_class)
        self.price_area = PriceAreaAPI(client, view_by_read_class)
        self.price_production = PriceProductionAPI(client, view_by_read_class)
        self.shop_result = SHOPResultAPI(client, view_by_read_class)
        self.shop_time_series = SHOPTimeSeriesAPI(client, view_by_read_class)
        self.scenario = ScenarioAPI(client, view_by_read_class)

    def graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> GraphQLList:
        """Execute a GraphQl query against the frontend_DayAheadBid data model.

        Args:
            query (str): The GraphQL query to issue.
            variables (dict[str, Any] | None): An optional dict of variables to pass to the query.
        """
        data_model_id = dm.DataModelId("sp_powerops_models_temp", "frontend_DayAheadBid", "1")
        result = self._client.data_modeling.graphql.query(data_model_id, query, variables)
        return GraphQLQueryResponse(data_model_id).parse(result)


class PowerOpsModelsV1Client:
    """
    PowerOpsModelsV1Client

    Generated with:
        pygen = 0.99.17
        cognite-sdk = 7.26.2
        pydantic = 2.6.4

    """

    def __init__(self, config_or_client: CogniteClient | ClientConfig):
        if isinstance(config_or_client, CogniteClient):
            client = config_or_client
        elif isinstance(config_or_client, ClientConfig):
            client = CogniteClient(config_or_client)
        else:
            raise ValueError(f"Expected CogniteClient or ClientConfig, got {type(config_or_client)}")
        # The client name is used for aggregated logging of Pygen Usage
        client.config.client_name = "CognitePygen:0.99.17"

        self.shop_based_day_ahead_bid_process = SHOPBasedDayAheadBidProcesAPIs(client)
        self.total_bid_matrix_calculation = TotalBidMatrixCalculationAPIs(client)
        self.water_value_based_day_ahead_bid_process = WaterValueBasedDayAheadBidProcesAPIs(client)
        self.day_ahead_configuration = DayAheadConfigurationAPIs(client)
        self.afrr_bid = AFRRBidAPIs(client)
        self.power_asset = PowerAssetAPIs(client)
        self.day_ahead_bid = DayAheadBidAPIs(client)

        self._client = client
        self._view_by_read_class = {
            k: v
            for api in [
                self.shop_based_day_ahead_bid_process,
                self.total_bid_matrix_calculation,
                self.water_value_based_day_ahead_bid_process,
                self.day_ahead_configuration,
                self.afrr_bid,
                self.power_asset,
                self.day_ahead_bid,
            ]
            for k, v in api._view_by_read_class.items()
        }

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
        if isinstance(items, data_classes.DomainModelWrite):
            instances = items.to_instances_write(self._view_by_read_class, write_none, allow_version_increase)
        else:
            instances = data_classes.ResourcesWrite()
            cache: set[tuple[str, str]] = set()
            for item in items:
                instances.extend(
                    item._to_instances_write(
                        cache,
                        self._view_by_read_class,
                        write_none,
                        allow_version_increase,
                    )
                )
        result = self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )
        time_series = []
        if instances.time_series:
            time_series = self._client.time_series.upsert(instances.time_series, mode="patch")

        return data_classes.ResourcesWriteResult(result.nodes, result.edges, TimeSeriesList(time_series))

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
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more items.

        Args:
            external_id: External id of the item(s) to delete.
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
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
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
