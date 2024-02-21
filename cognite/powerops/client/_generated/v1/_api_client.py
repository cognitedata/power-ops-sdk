from __future__ import annotations

import warnings
from pathlib import Path
from typing import Sequence

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.data_classes import TimeSeriesList
from cognite.client.credentials import OAuthClientCredentials

from ._api.alert import AlertAPI
from ._api.basic_bid_matrix import BasicBidMatrixAPI
from ._api.bid_calculation_task import BidCalculationTaskAPI
from ._api.bid_configuration import BidConfigurationAPI
from ._api.bid_configuration_shop import BidConfigurationShopAPI
from ._api.bid_configuration_water import BidConfigurationWaterAPI
from ._api.bid_document_afrr import BidDocumentAFRRAPI
from ._api.bid_document_day_ahead import BidDocumentDayAheadAPI
from ._api.bid_matrix import BidMatrixAPI
from ._api.bid_matrix_raw import BidMatrixRawAPI
from ._api.bid_method import BidMethodAPI
from ._api.bid_method_afrr import BidMethodAFRRAPI
from ._api.bid_method_custom import BidMethodCustomAPI
from ._api.bid_method_day_ahead import BidMethodDayAheadAPI
from ._api.bid_method_shop_multi_scenario import BidMethodSHOPMultiScenarioAPI
from ._api.bid_method_water_value import BidMethodWaterValueAPI
from ._api.bid_row import BidRowAPI
from ._api.custom_bid_matrix import CustomBidMatrixAPI
from ._api.generator import GeneratorAPI
from ._api.generator_efficiency_curve import GeneratorEfficiencyCurveAPI
from ._api.mapping import MappingAPI
from ._api.market_configuration import MarketConfigurationAPI
from ._api.model_template import ModelTemplateAPI
from ._api.multi_scenario_matrix import MultiScenarioMatrixAPI
from ._api.multi_scenario_matrix_raw import MultiScenarioMatrixRawAPI
from ._api.partial_post_processing_input import PartialPostProcessingInputAPI
from ._api.partial_post_processing_output import PartialPostProcessingOutputAPI
from ._api.plant import PlantAPI
from ._api.plant_shop import PlantShopAPI
from ._api.preprocessor_input import PreprocessorInputAPI
from ._api.preprocessor_output import PreprocessorOutputAPI
from ._api.price_area import PriceAreaAPI
from ._api.price_area_afrr import PriceAreaAFRRAPI
from ._api.price_area_asset import PriceAreaAssetAPI
from ._api.price_scenario import PriceScenarioAPI
from ._api.reservoir import ReservoirAPI
from ._api.shop_result import SHOPResultAPI
from ._api.shop_trigger_input import SHOPTriggerInputAPI
from ._api.shop_trigger_output import SHOPTriggerOutputAPI
from ._api.scenario import ScenarioAPI
from ._api.scenario_raw import ScenarioRawAPI
from ._api.shop_partial_bid_calculation_input import ShopPartialBidCalculationInputAPI
from ._api.shop_partial_bid_calculation_output import ShopPartialBidCalculationOutputAPI
from ._api.task_dispatcher_shop_input import TaskDispatcherShopInputAPI
from ._api.task_dispatcher_shop_output import TaskDispatcherShopOutputAPI
from ._api.task_dispatcher_water_input import TaskDispatcherWaterInputAPI
from ._api.task_dispatcher_water_output import TaskDispatcherWaterOutputAPI
from ._api.total_bid_matrix_calculation_input import TotalBidMatrixCalculationInputAPI
from ._api.total_bid_matrix_calculation_output import TotalBidMatrixCalculationOutputAPI
from ._api.turbine_efficiency_curve import TurbineEfficiencyCurveAPI
from ._api.water_partial_bid_calculation_input import WaterPartialBidCalculationInputAPI
from ._api.water_partial_bid_calculation_output import WaterPartialBidCalculationOutputAPI
from ._api.watercourse import WatercourseAPI
from ._api.watercourse_shop import WatercourseShopAPI
from ._api._core import SequenceNotStr
from .data_classes._core import DEFAULT_INSTANCE_SPACE
from . import data_classes


class SHOPBasedDayAheadBidProcesAPIs:
    """
    SHOPBasedDayAheadBidProcesAPIs

    Data Model:
        space: sp_powerops_models
        externalId: compute_SHOPBasedDayAhead
        version: 1

    """

    def __init__(self, client: CogniteClient):
        view_by_read_class = {
            data_classes.Alert: dm.ViewId("sp_powerops_models", "Alert", "1"),
            data_classes.BidConfigurationShop: dm.ViewId("sp_powerops_models", "BidConfigurationShop", "1"),
            data_classes.BidMatrixRaw: dm.ViewId("sp_powerops_models", "BidMatrixRaw", "1"),
            data_classes.BidMethodSHOPMultiScenario: dm.ViewId("sp_powerops_models", "BidMethodSHOPMultiScenario", "1"),
            data_classes.Mapping: dm.ViewId("sp_powerops_models", "Mapping", "1"),
            data_classes.MarketConfiguration: dm.ViewId("sp_powerops_models", "MarketConfiguration", "1"),
            data_classes.ModelTemplate: dm.ViewId("sp_powerops_models", "ModelTemplate", "1"),
            data_classes.MultiScenarioMatrixRaw: dm.ViewId("sp_powerops_models", "MultiScenarioMatrixRaw", "1"),
            data_classes.PlantShop: dm.ViewId("sp_powerops_models", "PlantShop", "1"),
            data_classes.PreprocessorInput: dm.ViewId("sp_powerops_models", "PreprocessorInput", "1"),
            data_classes.PreprocessorOutput: dm.ViewId("sp_powerops_models", "PreprocessorOutput", "1"),
            data_classes.PriceArea: dm.ViewId("sp_powerops_models", "PriceArea", "1"),
            data_classes.PriceScenario: dm.ViewId("sp_powerops_models", "PriceScenario", "1"),
            data_classes.SHOPResult: dm.ViewId("sp_powerops_models", "SHOPResult", "1"),
            data_classes.SHOPTriggerInput: dm.ViewId("sp_powerops_models", "SHOPTriggerInput", "1"),
            data_classes.SHOPTriggerOutput: dm.ViewId("sp_powerops_models", "SHOPTriggerOutput", "1"),
            data_classes.Scenario: dm.ViewId("sp_powerops_models", "Scenario", "1"),
            data_classes.ScenarioRaw: dm.ViewId("sp_powerops_models", "ScenarioRaw", "1"),
            data_classes.ShopPartialBidCalculationInput: dm.ViewId(
                "sp_powerops_models", "ShopPartialBidCalculationInput", "1"
            ),
            data_classes.ShopPartialBidCalculationOutput: dm.ViewId(
                "sp_powerops_models", "ShopPartialBidCalculationOutput", "1"
            ),
            data_classes.TaskDispatcherShopInput: dm.ViewId("sp_powerops_models", "TaskDispatcherShopInput", "1"),
            data_classes.TaskDispatcherShopOutput: dm.ViewId("sp_powerops_models", "TaskDispatcherShopOutput", "1"),
            data_classes.WatercourseShop: dm.ViewId("sp_powerops_models", "WatercourseShop", "1"),
        }
        self._view_by_read_class = view_by_read_class

        self.alert = AlertAPI(client, view_by_read_class)
        self.bid_configuration_shop = BidConfigurationShopAPI(client, view_by_read_class)
        self.bid_matrix_raw = BidMatrixRawAPI(client, view_by_read_class)
        self.bid_method_shop_multi_scenario = BidMethodSHOPMultiScenarioAPI(client, view_by_read_class)
        self.mapping = MappingAPI(client, view_by_read_class)
        self.market_configuration = MarketConfigurationAPI(client, view_by_read_class)
        self.model_template = ModelTemplateAPI(client, view_by_read_class)
        self.multi_scenario_matrix_raw = MultiScenarioMatrixRawAPI(client, view_by_read_class)
        self.plant_shop = PlantShopAPI(client, view_by_read_class)
        self.preprocessor_input = PreprocessorInputAPI(client, view_by_read_class)
        self.preprocessor_output = PreprocessorOutputAPI(client, view_by_read_class)
        self.price_area = PriceAreaAPI(client, view_by_read_class)
        self.price_scenario = PriceScenarioAPI(client, view_by_read_class)
        self.shop_result = SHOPResultAPI(client, view_by_read_class)
        self.shop_trigger_input = SHOPTriggerInputAPI(client, view_by_read_class)
        self.shop_trigger_output = SHOPTriggerOutputAPI(client, view_by_read_class)
        self.scenario = ScenarioAPI(client, view_by_read_class)
        self.scenario_raw = ScenarioRawAPI(client, view_by_read_class)
        self.shop_partial_bid_calculation_input = ShopPartialBidCalculationInputAPI(client, view_by_read_class)
        self.shop_partial_bid_calculation_output = ShopPartialBidCalculationOutputAPI(client, view_by_read_class)
        self.task_dispatcher_shop_input = TaskDispatcherShopInputAPI(client, view_by_read_class)
        self.task_dispatcher_shop_output = TaskDispatcherShopOutputAPI(client, view_by_read_class)
        self.watercourse_shop = WatercourseShopAPI(client, view_by_read_class)


class TotalBidCalculationAPIs:
    """
    TotalBidCalculationAPIs

    Data Model:
        space: sp_powerops_models
        externalId: compute_TotalBidCalculation
        version: 1

    """

    def __init__(self, client: CogniteClient):
        view_by_read_class = {
            data_classes.Alert: dm.ViewId("sp_powerops_models", "Alert", "1"),
            data_classes.BidDocumentDayAhead: dm.ViewId("sp_powerops_models", "BidDocumentDayAhead", "1"),
            data_classes.BidMatrix: dm.ViewId("sp_powerops_models", "BidMatrix", "1"),
            data_classes.BidMatrixRaw: dm.ViewId("sp_powerops_models", "BidMatrixRaw", "1"),
            data_classes.BidMethodDayAhead: dm.ViewId("sp_powerops_models", "BidMethodDayAhead", "1"),
            data_classes.BidMethodSHOPMultiScenario: dm.ViewId("sp_powerops_models", "BidMethodSHOPMultiScenario", "1"),
            data_classes.BidMethodWaterValue: dm.ViewId("sp_powerops_models", "BidMethodWaterValue", "1"),
            data_classes.Mapping: dm.ViewId("sp_powerops_models", "Mapping", "1"),
            data_classes.MarketConfiguration: dm.ViewId("sp_powerops_models", "MarketConfiguration", "1"),
            data_classes.ModelTemplate: dm.ViewId("sp_powerops_models", "ModelTemplate", "1"),
            data_classes.MultiScenarioMatrix: dm.ViewId("sp_powerops_models", "MultiScenarioMatrix", "1"),
            data_classes.PartialPostProcessingInput: dm.ViewId("sp_powerops_models", "PartialPostProcessingInput", "1"),
            data_classes.PartialPostProcessingOutput: dm.ViewId(
                "sp_powerops_models", "PartialPostProcessingOutput", "1"
            ),
            data_classes.PriceArea: dm.ViewId("sp_powerops_models", "PriceArea", "1"),
            data_classes.PriceScenario: dm.ViewId("sp_powerops_models", "PriceScenario", "1"),
            data_classes.SHOPResult: dm.ViewId("sp_powerops_models", "SHOPResult", "1"),
            data_classes.Scenario: dm.ViewId("sp_powerops_models", "Scenario", "1"),
            data_classes.TotalBidMatrixCalculationInput: dm.ViewId(
                "sp_powerops_models", "TotalBidMatrixCalculationInput", "1"
            ),
            data_classes.TotalBidMatrixCalculationOutput: dm.ViewId(
                "sp_powerops_models", "TotalBidMatrixCalculationOutput", "1"
            ),
            data_classes.WatercourseShop: dm.ViewId("sp_powerops_models", "WatercourseShop", "1"),
        }
        self._view_by_read_class = view_by_read_class

        self.alert = AlertAPI(client, view_by_read_class)
        self.bid_document_day_ahead = BidDocumentDayAheadAPI(client, view_by_read_class)
        self.bid_matrix = BidMatrixAPI(client, view_by_read_class)
        self.bid_matrix_raw = BidMatrixRawAPI(client, view_by_read_class)
        self.bid_method_day_ahead = BidMethodDayAheadAPI(client, view_by_read_class)
        self.bid_method_shop_multi_scenario = BidMethodSHOPMultiScenarioAPI(client, view_by_read_class)
        self.bid_method_water_value = BidMethodWaterValueAPI(client, view_by_read_class)
        self.mapping = MappingAPI(client, view_by_read_class)
        self.market_configuration = MarketConfigurationAPI(client, view_by_read_class)
        self.model_template = ModelTemplateAPI(client, view_by_read_class)
        self.multi_scenario_matrix = MultiScenarioMatrixAPI(client, view_by_read_class)
        self.partial_post_processing_input = PartialPostProcessingInputAPI(client, view_by_read_class)
        self.partial_post_processing_output = PartialPostProcessingOutputAPI(client, view_by_read_class)
        self.price_area = PriceAreaAPI(client, view_by_read_class)
        self.price_scenario = PriceScenarioAPI(client, view_by_read_class)
        self.shop_result = SHOPResultAPI(client, view_by_read_class)
        self.scenario = ScenarioAPI(client, view_by_read_class)
        self.total_bid_matrix_calculation_input = TotalBidMatrixCalculationInputAPI(client, view_by_read_class)
        self.total_bid_matrix_calculation_output = TotalBidMatrixCalculationOutputAPI(client, view_by_read_class)
        self.watercourse_shop = WatercourseShopAPI(client, view_by_read_class)


class WaterValueBasedDayAheadBidProcesAPIs:
    """
    WaterValueBasedDayAheadBidProcesAPIs

    Data Model:
        space: sp_powerops_models
        externalId: compute_WaterValueBasedDayAheadBid
        version: 1

    """

    def __init__(self, client: CogniteClient):
        view_by_read_class = {
            data_classes.Alert: dm.ViewId("sp_powerops_models", "Alert", "1"),
            data_classes.BidCalculationTask: dm.ViewId("sp_powerops_models", "BidCalculationTask", "1"),
            data_classes.BidConfigurationWater: dm.ViewId("sp_powerops_models", "BidConfigurationWater", "1"),
            data_classes.BidMatrixRaw: dm.ViewId("sp_powerops_models", "BidMatrixRaw", "1"),
            data_classes.BidMethodWaterValue: dm.ViewId("sp_powerops_models", "BidMethodWaterValue", "1"),
            data_classes.Generator: dm.ViewId("sp_powerops_models", "Generator", "1"),
            data_classes.GeneratorEfficiencyCurve: dm.ViewId("sp_powerops_models", "GeneratorEfficiencyCurve", "1"),
            data_classes.MarketConfiguration: dm.ViewId("sp_powerops_models", "MarketConfiguration", "1"),
            data_classes.Plant: dm.ViewId("sp_powerops_models", "Plant", "1"),
            data_classes.PriceArea: dm.ViewId("sp_powerops_models", "PriceArea", "1"),
            data_classes.PriceScenario: dm.ViewId("sp_powerops_models", "PriceScenario", "1"),
            data_classes.Reservoir: dm.ViewId("sp_powerops_models", "Reservoir", "1"),
            data_classes.TaskDispatcherWaterInput: dm.ViewId("sp_powerops_models", "TaskDispatcherWaterInput", "1"),
            data_classes.TaskDispatcherWaterOutput: dm.ViewId("sp_powerops_models", "TaskDispatcherWaterOutput", "1"),
            data_classes.TurbineEfficiencyCurve: dm.ViewId("sp_powerops_models", "TurbineEfficiencyCurve", "1"),
            data_classes.WaterPartialBidCalculationInput: dm.ViewId(
                "sp_powerops_models", "WaterPartialBidCalculationInput", "1"
            ),
            data_classes.WaterPartialBidCalculationOutput: dm.ViewId(
                "sp_powerops_models", "WaterPartialBidCalculationOutput", "1"
            ),
            data_classes.Watercourse: dm.ViewId("sp_powerops_models", "Watercourse", "1"),
        }
        self._view_by_read_class = view_by_read_class

        self.alert = AlertAPI(client, view_by_read_class)
        self.bid_calculation_task = BidCalculationTaskAPI(client, view_by_read_class)
        self.bid_configuration_water = BidConfigurationWaterAPI(client, view_by_read_class)
        self.bid_matrix_raw = BidMatrixRawAPI(client, view_by_read_class)
        self.bid_method_water_value = BidMethodWaterValueAPI(client, view_by_read_class)
        self.generator = GeneratorAPI(client, view_by_read_class)
        self.generator_efficiency_curve = GeneratorEfficiencyCurveAPI(client, view_by_read_class)
        self.market_configuration = MarketConfigurationAPI(client, view_by_read_class)
        self.plant = PlantAPI(client, view_by_read_class)
        self.price_area = PriceAreaAPI(client, view_by_read_class)
        self.price_scenario = PriceScenarioAPI(client, view_by_read_class)
        self.reservoir = ReservoirAPI(client, view_by_read_class)
        self.task_dispatcher_water_input = TaskDispatcherWaterInputAPI(client, view_by_read_class)
        self.task_dispatcher_water_output = TaskDispatcherWaterOutputAPI(client, view_by_read_class)
        self.turbine_efficiency_curve = TurbineEfficiencyCurveAPI(client, view_by_read_class)
        self.water_partial_bid_calculation_input = WaterPartialBidCalculationInputAPI(client, view_by_read_class)
        self.water_partial_bid_calculation_output = WaterPartialBidCalculationOutputAPI(client, view_by_read_class)
        self.watercourse = WatercourseAPI(client, view_by_read_class)


class DayAheadConfigurationAPIs:
    """
    DayAheadConfigurationAPIs

    Data Model:
        space: sp_powerops_models
        externalId: config_DayAheadConfiguration
        version: 1

    """

    def __init__(self, client: CogniteClient):
        view_by_read_class = {
            data_classes.BidConfiguration: dm.ViewId("sp_powerops_models", "BidConfiguration", "1"),
            data_classes.BidConfigurationShop: dm.ViewId("sp_powerops_models", "BidConfigurationShop", "1"),
            data_classes.BidConfigurationWater: dm.ViewId("sp_powerops_models", "BidConfigurationWater", "1"),
            data_classes.BidMethod: dm.ViewId("sp_powerops_models", "BidMethod", "1"),
            data_classes.BidMethodDayAhead: dm.ViewId("sp_powerops_models", "BidMethodDayAhead", "1"),
            data_classes.BidMethodSHOPMultiScenario: dm.ViewId("sp_powerops_models", "BidMethodSHOPMultiScenario", "1"),
            data_classes.BidMethodWaterValue: dm.ViewId("sp_powerops_models", "BidMethodWaterValue", "1"),
            data_classes.Generator: dm.ViewId("sp_powerops_models", "Generator", "1"),
            data_classes.GeneratorEfficiencyCurve: dm.ViewId("sp_powerops_models", "GeneratorEfficiencyCurve", "1"),
            data_classes.MarketConfiguration: dm.ViewId("sp_powerops_models", "MarketConfiguration", "1"),
            data_classes.Plant: dm.ViewId("sp_powerops_models", "Plant", "1"),
            data_classes.PlantShop: dm.ViewId("sp_powerops_models", "PlantShop", "1"),
            data_classes.PriceArea: dm.ViewId("sp_powerops_models", "PriceArea", "1"),
            data_classes.PriceScenario: dm.ViewId("sp_powerops_models", "PriceScenario", "1"),
            data_classes.Reservoir: dm.ViewId("sp_powerops_models", "Reservoir", "1"),
            data_classes.TurbineEfficiencyCurve: dm.ViewId("sp_powerops_models", "TurbineEfficiencyCurve", "1"),
            data_classes.Watercourse: dm.ViewId("sp_powerops_models", "Watercourse", "1"),
            data_classes.WatercourseShop: dm.ViewId("sp_powerops_models", "WatercourseShop", "1"),
        }
        self._view_by_read_class = view_by_read_class

        self.bid_configuration = BidConfigurationAPI(client, view_by_read_class)
        self.bid_configuration_shop = BidConfigurationShopAPI(client, view_by_read_class)
        self.bid_configuration_water = BidConfigurationWaterAPI(client, view_by_read_class)
        self.bid_method = BidMethodAPI(client, view_by_read_class)
        self.bid_method_day_ahead = BidMethodDayAheadAPI(client, view_by_read_class)
        self.bid_method_shop_multi_scenario = BidMethodSHOPMultiScenarioAPI(client, view_by_read_class)
        self.bid_method_water_value = BidMethodWaterValueAPI(client, view_by_read_class)
        self.generator = GeneratorAPI(client, view_by_read_class)
        self.generator_efficiency_curve = GeneratorEfficiencyCurveAPI(client, view_by_read_class)
        self.market_configuration = MarketConfigurationAPI(client, view_by_read_class)
        self.plant = PlantAPI(client, view_by_read_class)
        self.plant_shop = PlantShopAPI(client, view_by_read_class)
        self.price_area = PriceAreaAPI(client, view_by_read_class)
        self.price_scenario = PriceScenarioAPI(client, view_by_read_class)
        self.reservoir = ReservoirAPI(client, view_by_read_class)
        self.turbine_efficiency_curve = TurbineEfficiencyCurveAPI(client, view_by_read_class)
        self.watercourse = WatercourseAPI(client, view_by_read_class)
        self.watercourse_shop = WatercourseShopAPI(client, view_by_read_class)


class AFRRBidAPIs:
    """
    AFRRBidAPIs

    Data Model:
        space: sp_powerops_models
        externalId: frontend_AFRRBid
        version: 1

    """

    def __init__(self, client: CogniteClient):
        view_by_read_class = {
            data_classes.Alert: dm.ViewId("sp_powerops_models", "Alert", "1"),
            data_classes.BidDocumentAFRR: dm.ViewId("sp_powerops_models", "BidDocumentAFRR", "1"),
            data_classes.BidMethodAFRR: dm.ViewId("sp_powerops_models", "BidMethodAFRR", "1"),
            data_classes.BidRow: dm.ViewId("sp_powerops_models", "BidRow", "1"),
            data_classes.PriceAreaAFRR: dm.ViewId("sp_powerops_models", "PriceAreaAFRR", "1"),
        }
        self._view_by_read_class = view_by_read_class

        self.alert = AlertAPI(client, view_by_read_class)
        self.bid_document_afrr = BidDocumentAFRRAPI(client, view_by_read_class)
        self.bid_method_afrr = BidMethodAFRRAPI(client, view_by_read_class)
        self.bid_row = BidRowAPI(client, view_by_read_class)
        self.price_area_afrr = PriceAreaAFRRAPI(client, view_by_read_class)


class PowerAssetAPIs:
    """
    PowerAssetAPIs

    Data Model:
        space: sp_powerops_models
        externalId: frontend_Asset
        version: 1

    """

    def __init__(self, client: CogniteClient):
        view_by_read_class = {
            data_classes.BidMethodDayAhead: dm.ViewId("sp_powerops_models", "BidMethodDayAhead", "1"),
            data_classes.Generator: dm.ViewId("sp_powerops_models", "Generator", "1"),
            data_classes.GeneratorEfficiencyCurve: dm.ViewId("sp_powerops_models", "GeneratorEfficiencyCurve", "1"),
            data_classes.Plant: dm.ViewId("sp_powerops_models", "Plant", "1"),
            data_classes.PriceAreaAsset: dm.ViewId("sp_powerops_models", "PriceAreaAsset", "1"),
            data_classes.PriceScenario: dm.ViewId("sp_powerops_models", "PriceScenario", "1"),
            data_classes.Reservoir: dm.ViewId("sp_powerops_models", "Reservoir", "1"),
            data_classes.TurbineEfficiencyCurve: dm.ViewId("sp_powerops_models", "TurbineEfficiencyCurve", "1"),
            data_classes.Watercourse: dm.ViewId("sp_powerops_models", "Watercourse", "1"),
        }
        self._view_by_read_class = view_by_read_class

        self.bid_method_day_ahead = BidMethodDayAheadAPI(client, view_by_read_class)
        self.generator = GeneratorAPI(client, view_by_read_class)
        self.generator_efficiency_curve = GeneratorEfficiencyCurveAPI(client, view_by_read_class)
        self.plant = PlantAPI(client, view_by_read_class)
        self.price_area_asset = PriceAreaAssetAPI(client, view_by_read_class)
        self.price_scenario = PriceScenarioAPI(client, view_by_read_class)
        self.reservoir = ReservoirAPI(client, view_by_read_class)
        self.turbine_efficiency_curve = TurbineEfficiencyCurveAPI(client, view_by_read_class)
        self.watercourse = WatercourseAPI(client, view_by_read_class)


class DayAheadBidAPIs:
    """
    DayAheadBidAPIs

    Data Model:
        space: sp_powerops_models
        externalId: frontend_DayAheadBid
        version: 1

    """

    def __init__(self, client: CogniteClient):
        view_by_read_class = {
            data_classes.Alert: dm.ViewId("sp_powerops_models", "Alert", "1"),
            data_classes.BasicBidMatrix: dm.ViewId("sp_powerops_models", "BasicBidMatrix", "1"),
            data_classes.BidDocumentDayAhead: dm.ViewId("sp_powerops_models", "BidDocumentDayAhead", "1"),
            data_classes.BidMatrix: dm.ViewId("sp_powerops_models", "BidMatrix", "1"),
            data_classes.BidMethodCustom: dm.ViewId("sp_powerops_models", "BidMethodCustom", "1"),
            data_classes.BidMethodDayAhead: dm.ViewId("sp_powerops_models", "BidMethodDayAhead", "1"),
            data_classes.BidMethodSHOPMultiScenario: dm.ViewId("sp_powerops_models", "BidMethodSHOPMultiScenario", "1"),
            data_classes.BidMethodWaterValue: dm.ViewId("sp_powerops_models", "BidMethodWaterValue", "1"),
            data_classes.CustomBidMatrix: dm.ViewId("sp_powerops_models", "CustomBidMatrix", "1"),
            data_classes.Mapping: dm.ViewId("sp_powerops_models", "Mapping", "1"),
            data_classes.ModelTemplate: dm.ViewId("sp_powerops_models", "ModelTemplate", "1"),
            data_classes.MultiScenarioMatrix: dm.ViewId("sp_powerops_models", "MultiScenarioMatrix", "1"),
            data_classes.PriceArea: dm.ViewId("sp_powerops_models", "PriceArea", "1"),
            data_classes.PriceScenario: dm.ViewId("sp_powerops_models", "PriceScenario", "1"),
            data_classes.SHOPResult: dm.ViewId("sp_powerops_models", "SHOPResult", "1"),
            data_classes.Scenario: dm.ViewId("sp_powerops_models", "Scenario", "1"),
            data_classes.WatercourseShop: dm.ViewId("sp_powerops_models", "WatercourseShop", "1"),
        }
        self._view_by_read_class = view_by_read_class

        self.alert = AlertAPI(client, view_by_read_class)
        self.basic_bid_matrix = BasicBidMatrixAPI(client, view_by_read_class)
        self.bid_document_day_ahead = BidDocumentDayAheadAPI(client, view_by_read_class)
        self.bid_matrix = BidMatrixAPI(client, view_by_read_class)
        self.bid_method_custom = BidMethodCustomAPI(client, view_by_read_class)
        self.bid_method_day_ahead = BidMethodDayAheadAPI(client, view_by_read_class)
        self.bid_method_shop_multi_scenario = BidMethodSHOPMultiScenarioAPI(client, view_by_read_class)
        self.bid_method_water_value = BidMethodWaterValueAPI(client, view_by_read_class)
        self.custom_bid_matrix = CustomBidMatrixAPI(client, view_by_read_class)
        self.mapping = MappingAPI(client, view_by_read_class)
        self.model_template = ModelTemplateAPI(client, view_by_read_class)
        self.multi_scenario_matrix = MultiScenarioMatrixAPI(client, view_by_read_class)
        self.price_area = PriceAreaAPI(client, view_by_read_class)
        self.price_scenario = PriceScenarioAPI(client, view_by_read_class)
        self.shop_result = SHOPResultAPI(client, view_by_read_class)
        self.scenario = ScenarioAPI(client, view_by_read_class)
        self.watercourse_shop = WatercourseShopAPI(client, view_by_read_class)


class PowerOpsModelsV1Client:
    """
    PowerOpsModelsV1Client

    Generated with:
        pygen = 0.99.10
        cognite-sdk = 7.20.1
        pydantic = 2.6.1

    """

    def __init__(self, config_or_client: CogniteClient | ClientConfig):
        if isinstance(config_or_client, CogniteClient):
            client = config_or_client
        elif isinstance(config_or_client, ClientConfig):
            client = CogniteClient(config_or_client)
        else:
            raise ValueError(f"Expected CogniteClient or ClientConfig, got {type(config_or_client)}")
        # The client name is used for aggregated logging of Pygen Usage
        client.config.client_name = "CognitePygen:0.99.10"

        self.shop_based_day_ahead_bid_process = SHOPBasedDayAheadBidProcesAPIs(client)
        self.total_bid_calculation = TotalBidCalculationAPIs(client)
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
                self.total_bid_calculation,
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
        if isinstance(items, data_classes.DomainModelWrite):
            instances = items.to_instances_write(self._view_by_read_class, write_none)
        else:
            instances = data_classes.ResourcesWrite()
            cache: set[tuple[str, str]] = set()
            for item in items:
                instances.extend(item._to_instances_write(cache, self._view_by_read_class, write_none))
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

                >>> from omni import OmniClient
                >>> client = OmniClient()
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
