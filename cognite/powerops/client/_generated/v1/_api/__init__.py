from cognite.powerops.client._generated.v1._api.alert import AlertAPI
from cognite.powerops.client._generated.v1._api.benchmarking_calculation_input import BenchmarkingCalculationInputAPI
from cognite.powerops.client._generated.v1._api.benchmarking_calculation_input_shop_results import BenchmarkingCalculationInputShopResultsAPI
from cognite.powerops.client._generated.v1._api.benchmarking_calculation_output import BenchmarkingCalculationOutputAPI
from cognite.powerops.client._generated.v1._api.benchmarking_calculation_output_alerts import BenchmarkingCalculationOutputAlertsAPI
from cognite.powerops.client._generated.v1._api.benchmarking_calculation_output_benchmarking_results import BenchmarkingCalculationOutputBenchmarkingResultsAPI
from cognite.powerops.client._generated.v1._api.benchmarking_configuration_day_ahead import BenchmarkingConfigurationDayAheadAPI
from cognite.powerops.client._generated.v1._api.benchmarking_configuration_day_ahead_assets_per_shop_model import BenchmarkingConfigurationDayAheadAssetsPerShopModelAPI
from cognite.powerops.client._generated.v1._api.benchmarking_configuration_day_ahead_bid_configurations import BenchmarkingConfigurationDayAheadBidConfigurationsAPI
from cognite.powerops.client._generated.v1._api.benchmarking_production_obligation_day_ahead import BenchmarkingProductionObligationDayAheadAPI
from cognite.powerops.client._generated.v1._api.benchmarking_result_day_ahead import BenchmarkingResultDayAheadAPI
from cognite.powerops.client._generated.v1._api.benchmarking_result_day_ahead_alerts import BenchmarkingResultDayAheadAlertsAPI
from cognite.powerops.client._generated.v1._api.benchmarking_shop_case import BenchmarkingShopCaseAPI
from cognite.powerops.client._generated.v1._api.benchmarking_shop_case_shop_files import BenchmarkingShopCaseShopFilesAPI
from cognite.powerops.client._generated.v1._api.benchmarking_task_dispatcher_input_day_ahead import BenchmarkingTaskDispatcherInputDayAheadAPI
from cognite.powerops.client._generated.v1._api.benchmarking_task_dispatcher_output_day_ahead import BenchmarkingTaskDispatcherOutputDayAheadAPI
from cognite.powerops.client._generated.v1._api.benchmarking_task_dispatcher_output_day_ahead_alerts import BenchmarkingTaskDispatcherOutputDayAheadAlertsAPI
from cognite.powerops.client._generated.v1._api.benchmarking_task_dispatcher_output_day_ahead_benchmarking_sub_tasks import BenchmarkingTaskDispatcherOutputDayAheadBenchmarkingSubTasksAPI
from cognite.powerops.client._generated.v1._api.bid_configuration_day_ahead import BidConfigurationDayAheadAPI
from cognite.powerops.client._generated.v1._api.bid_configuration_day_ahead_partials import BidConfigurationDayAheadPartialsAPI
from cognite.powerops.client._generated.v1._api.bid_document_afrr import BidDocumentAFRRAPI
from cognite.powerops.client._generated.v1._api.bid_document_afrr_alerts import BidDocumentAFRRAlertsAPI
from cognite.powerops.client._generated.v1._api.bid_document_afrr_bids import BidDocumentAFRRBidsAPI
from cognite.powerops.client._generated.v1._api.bid_document import BidDocumentAPI
from cognite.powerops.client._generated.v1._api.bid_document_alerts import BidDocumentAlertsAPI
from cognite.powerops.client._generated.v1._api.bid_document_day_ahead import BidDocumentDayAheadAPI
from cognite.powerops.client._generated.v1._api.bid_document_day_ahead_alerts import BidDocumentDayAheadAlertsAPI
from cognite.powerops.client._generated.v1._api.bid_document_day_ahead_partials import BidDocumentDayAheadPartialsAPI
from cognite.powerops.client._generated.v1._api.bid_matrix import BidMatrixAPI
from cognite.powerops.client._generated.v1._api.bid_matrix_information import BidMatrixInformationAPI
from cognite.powerops.client._generated.v1._api.bid_matrix_information_alerts import BidMatrixInformationAlertsAPI
from cognite.powerops.client._generated.v1._api.bid_matrix_information_underlying_bid_matrices import BidMatrixInformationUnderlyingBidMatricesAPI
from cognite.powerops.client._generated.v1._api.bid_row import BidRowAPI
from cognite.powerops.client._generated.v1._api.bid_row_alerts import BidRowAlertsAPI
from cognite.powerops.client._generated.v1._api.date_specification import DateSpecificationAPI
from cognite.powerops.client._generated.v1._api.function_input import FunctionInputAPI
from cognite.powerops.client._generated.v1._api.function_output import FunctionOutputAPI
from cognite.powerops.client._generated.v1._api.function_output_alerts import FunctionOutputAlertsAPI
from cognite.powerops.client._generated.v1._api.generator import GeneratorAPI
from cognite.powerops.client._generated.v1._api.generator_efficiency_curve import GeneratorEfficiencyCurveAPI
from cognite.powerops.client._generated.v1._api.generator_turbine_efficiency_curves import GeneratorTurbineEfficiencyCurvesAPI
from cognite.powerops.client._generated.v1._api.market_configuration import MarketConfigurationAPI
from cognite.powerops.client._generated.v1._api.multi_scenario_partial_bid_matrix_calculation_input import MultiScenarioPartialBidMatrixCalculationInputAPI
from cognite.powerops.client._generated.v1._api.multi_scenario_partial_bid_matrix_calculation_input_price_production import MultiScenarioPartialBidMatrixCalculationInputPriceProductionAPI
from cognite.powerops.client._generated.v1._api.partial_bid_configuration import PartialBidConfigurationAPI
from cognite.powerops.client._generated.v1._api.partial_bid_matrix_calculation_input import PartialBidMatrixCalculationInputAPI
from cognite.powerops.client._generated.v1._api.partial_bid_matrix_calculation_output import PartialBidMatrixCalculationOutputAPI
from cognite.powerops.client._generated.v1._api.partial_bid_matrix_calculation_output_alerts import PartialBidMatrixCalculationOutputAlertsAPI
from cognite.powerops.client._generated.v1._api.partial_bid_matrix_information import PartialBidMatrixInformationAPI
from cognite.powerops.client._generated.v1._api.partial_bid_matrix_information_alerts import PartialBidMatrixInformationAlertsAPI
from cognite.powerops.client._generated.v1._api.partial_bid_matrix_information_underlying_bid_matrices import PartialBidMatrixInformationUnderlyingBidMatricesAPI
from cognite.powerops.client._generated.v1._api.partial_bid_matrix_information_with_scenarios import PartialBidMatrixInformationWithScenariosAPI
from cognite.powerops.client._generated.v1._api.partial_bid_matrix_information_with_scenarios_alerts import PartialBidMatrixInformationWithScenariosAlertsAPI
from cognite.powerops.client._generated.v1._api.partial_bid_matrix_information_with_scenarios_multi_scenario_input import PartialBidMatrixInformationWithScenariosMultiScenarioInputAPI
from cognite.powerops.client._generated.v1._api.partial_bid_matrix_information_with_scenarios_underlying_bid_matrices import PartialBidMatrixInformationWithScenariosUnderlyingBidMatricesAPI
from cognite.powerops.client._generated.v1._api.plant import PlantAPI
from cognite.powerops.client._generated.v1._api.plant_information import PlantInformationAPI
from cognite.powerops.client._generated.v1._api.plant_information_generators import PlantInformationGeneratorsAPI
from cognite.powerops.client._generated.v1._api.plant_water_value_based import PlantWaterValueBasedAPI
from cognite.powerops.client._generated.v1._api.plant_water_value_based_generators import PlantWaterValueBasedGeneratorsAPI
from cognite.powerops.client._generated.v1._api.power_asset import PowerAssetAPI
from cognite.powerops.client._generated.v1._api.price_area_afrr import PriceAreaAFRRAPI
from cognite.powerops.client._generated.v1._api.price_area import PriceAreaAPI
from cognite.powerops.client._generated.v1._api.price_area_day_ahead import PriceAreaDayAheadAPI
from cognite.powerops.client._generated.v1._api.price_area_information import PriceAreaInformationAPI
from cognite.powerops.client._generated.v1._api.price_production import PriceProductionAPI
from cognite.powerops.client._generated.v1._api.shop_attribute_mapping import ShopAttributeMappingAPI
from cognite.powerops.client._generated.v1._api.shop_based_partial_bid_configuration import ShopBasedPartialBidConfigurationAPI
from cognite.powerops.client._generated.v1._api.shop_case import ShopCaseAPI
from cognite.powerops.client._generated.v1._api.shop_case_shop_files import ShopCaseShopFilesAPI
from cognite.powerops.client._generated.v1._api.shop_commands import ShopCommandsAPI
from cognite.powerops.client._generated.v1._api.shop_file import ShopFileAPI
from cognite.powerops.client._generated.v1._api.shop_model import ShopModelAPI
from cognite.powerops.client._generated.v1._api.shop_model_base_attribute_mappings import ShopModelBaseAttributeMappingsAPI
from cognite.powerops.client._generated.v1._api.shop_model_cog_shop_files_config import ShopModelCogShopFilesConfigAPI
from cognite.powerops.client._generated.v1._api.shop_model_with_assets import ShopModelWithAssetsAPI
from cognite.powerops.client._generated.v1._api.shop_model_with_assets_power_assets import ShopModelWithAssetsPowerAssetsAPI
from cognite.powerops.client._generated.v1._api.shop_model_with_assets_production_obligations import ShopModelWithAssetsProductionObligationsAPI
from cognite.powerops.client._generated.v1._api.shop_output_time_series_definition import ShopOutputTimeSeriesDefinitionAPI
from cognite.powerops.client._generated.v1._api.shop_penalty_report import ShopPenaltyReportAPI
from cognite.powerops.client._generated.v1._api.shop_preprocessor_input import ShopPreprocessorInputAPI
from cognite.powerops.client._generated.v1._api.shop_preprocessor_output import ShopPreprocessorOutputAPI
from cognite.powerops.client._generated.v1._api.shop_preprocessor_output_alerts import ShopPreprocessorOutputAlertsAPI
from cognite.powerops.client._generated.v1._api.shop_result import ShopResultAPI
from cognite.powerops.client._generated.v1._api.shop_result_alerts import ShopResultAlertsAPI
from cognite.powerops.client._generated.v1._api.shop_result_output_time_series import ShopResultOutputTimeSeriesAPI
from cognite.powerops.client._generated.v1._api.shop_scenario import ShopScenarioAPI
from cognite.powerops.client._generated.v1._api.shop_scenario_attribute_mappings_override import ShopScenarioAttributeMappingsOverrideAPI
from cognite.powerops.client._generated.v1._api.shop_scenario_output_definition import ShopScenarioOutputDefinitionAPI
from cognite.powerops.client._generated.v1._api.shop_scenario_set import ShopScenarioSetAPI
from cognite.powerops.client._generated.v1._api.shop_scenario_set_scenarios import ShopScenarioSetScenariosAPI
from cognite.powerops.client._generated.v1._api.shop_time_resolution import ShopTimeResolutionAPI
from cognite.powerops.client._generated.v1._api.shop_time_series import ShopTimeSeriesAPI
from cognite.powerops.client._generated.v1._api.shop_trigger_input import ShopTriggerInputAPI
from cognite.powerops.client._generated.v1._api.shop_trigger_output import ShopTriggerOutputAPI
from cognite.powerops.client._generated.v1._api.shop_trigger_output_alerts import ShopTriggerOutputAlertsAPI
from cognite.powerops.client._generated.v1._api.task_dispatcher_input import TaskDispatcherInputAPI
from cognite.powerops.client._generated.v1._api.task_dispatcher_output import TaskDispatcherOutputAPI
from cognite.powerops.client._generated.v1._api.task_dispatcher_output_alerts import TaskDispatcherOutputAlertsAPI
from cognite.powerops.client._generated.v1._api.task_dispatcher_output_process_sub_tasks import TaskDispatcherOutputProcessSubTasksAPI
from cognite.powerops.client._generated.v1._api.total_bid_matrix_calculation_input import TotalBidMatrixCalculationInputAPI
from cognite.powerops.client._generated.v1._api.total_bid_matrix_calculation_input_partial_bid_matrices import TotalBidMatrixCalculationInputPartialBidMatricesAPI
from cognite.powerops.client._generated.v1._api.total_bid_matrix_calculation_output import TotalBidMatrixCalculationOutputAPI
from cognite.powerops.client._generated.v1._api.total_bid_matrix_calculation_output_alerts import TotalBidMatrixCalculationOutputAlertsAPI
from cognite.powerops.client._generated.v1._api.turbine_efficiency_curve import TurbineEfficiencyCurveAPI
from cognite.powerops.client._generated.v1._api.water_value_based_partial_bid_configuration import WaterValueBasedPartialBidConfigurationAPI
from cognite.powerops.client._generated.v1._api.water_value_based_partial_bid_matrix_calculation_input import WaterValueBasedPartialBidMatrixCalculationInputAPI
from cognite.powerops.client._generated.v1._api.watercourse import WatercourseAPI

__all__ = [
    "AlertAPI",
    "BenchmarkingCalculationInputAPI",
    "BenchmarkingCalculationInputShopResultsAPI",
    "BenchmarkingCalculationOutputAPI",
    "BenchmarkingCalculationOutputAlertsAPI",
    "BenchmarkingCalculationOutputBenchmarkingResultsAPI",
    "BenchmarkingConfigurationDayAheadAPI",
    "BenchmarkingConfigurationDayAheadAssetsPerShopModelAPI",
    "BenchmarkingConfigurationDayAheadBidConfigurationsAPI",
    "BenchmarkingProductionObligationDayAheadAPI",
    "BenchmarkingResultDayAheadAPI",
    "BenchmarkingResultDayAheadAlertsAPI",
    "BenchmarkingShopCaseAPI",
    "BenchmarkingShopCaseShopFilesAPI",
    "BenchmarkingTaskDispatcherInputDayAheadAPI",
    "BenchmarkingTaskDispatcherOutputDayAheadAPI",
    "BenchmarkingTaskDispatcherOutputDayAheadAlertsAPI",
    "BenchmarkingTaskDispatcherOutputDayAheadBenchmarkingSubTasksAPI",
    "BidConfigurationDayAheadAPI",
    "BidConfigurationDayAheadPartialsAPI",
    "BidDocumentAFRRAPI",
    "BidDocumentAFRRAlertsAPI",
    "BidDocumentAFRRBidsAPI",
    "BidDocumentAPI",
    "BidDocumentAlertsAPI",
    "BidDocumentDayAheadAPI",
    "BidDocumentDayAheadAlertsAPI",
    "BidDocumentDayAheadPartialsAPI",
    "BidMatrixAPI",
    "BidMatrixInformationAPI",
    "BidMatrixInformationAlertsAPI",
    "BidMatrixInformationUnderlyingBidMatricesAPI",
    "BidRowAPI",
    "BidRowAlertsAPI",
    "DateSpecificationAPI",
    "FunctionInputAPI",
    "FunctionOutputAPI",
    "FunctionOutputAlertsAPI",
    "GeneratorAPI",
    "GeneratorEfficiencyCurveAPI",
    "GeneratorTurbineEfficiencyCurvesAPI",
    "MarketConfigurationAPI",
    "MultiScenarioPartialBidMatrixCalculationInputAPI",
    "MultiScenarioPartialBidMatrixCalculationInputPriceProductionAPI",
    "PartialBidConfigurationAPI",
    "PartialBidMatrixCalculationInputAPI",
    "PartialBidMatrixCalculationOutputAPI",
    "PartialBidMatrixCalculationOutputAlertsAPI",
    "PartialBidMatrixInformationAPI",
    "PartialBidMatrixInformationAlertsAPI",
    "PartialBidMatrixInformationUnderlyingBidMatricesAPI",
    "PartialBidMatrixInformationWithScenariosAPI",
    "PartialBidMatrixInformationWithScenariosAlertsAPI",
    "PartialBidMatrixInformationWithScenariosMultiScenarioInputAPI",
    "PartialBidMatrixInformationWithScenariosUnderlyingBidMatricesAPI",
    "PlantAPI",
    "PlantInformationAPI",
    "PlantInformationGeneratorsAPI",
    "PlantWaterValueBasedAPI",
    "PlantWaterValueBasedGeneratorsAPI",
    "PowerAssetAPI",
    "PriceAreaAFRRAPI",
    "PriceAreaAPI",
    "PriceAreaDayAheadAPI",
    "PriceAreaInformationAPI",
    "PriceProductionAPI",
    "ShopAttributeMappingAPI",
    "ShopBasedPartialBidConfigurationAPI",
    "ShopCaseAPI",
    "ShopCaseShopFilesAPI",
    "ShopCommandsAPI",
    "ShopFileAPI",
    "ShopModelAPI",
    "ShopModelBaseAttributeMappingsAPI",
    "ShopModelCogShopFilesConfigAPI",
    "ShopModelWithAssetsAPI",
    "ShopModelWithAssetsPowerAssetsAPI",
    "ShopModelWithAssetsProductionObligationsAPI",
    "ShopOutputTimeSeriesDefinitionAPI",
    "ShopPenaltyReportAPI",
    "ShopPreprocessorInputAPI",
    "ShopPreprocessorOutputAPI",
    "ShopPreprocessorOutputAlertsAPI",
    "ShopResultAPI",
    "ShopResultAlertsAPI",
    "ShopResultOutputTimeSeriesAPI",
    "ShopScenarioAPI",
    "ShopScenarioAttributeMappingsOverrideAPI",
    "ShopScenarioOutputDefinitionAPI",
    "ShopScenarioSetAPI",
    "ShopScenarioSetScenariosAPI",
    "ShopTimeResolutionAPI",
    "ShopTimeSeriesAPI",
    "ShopTriggerInputAPI",
    "ShopTriggerOutputAPI",
    "ShopTriggerOutputAlertsAPI",
    "TaskDispatcherInputAPI",
    "TaskDispatcherOutputAPI",
    "TaskDispatcherOutputAlertsAPI",
    "TaskDispatcherOutputProcessSubTasksAPI",
    "TotalBidMatrixCalculationInputAPI",
    "TotalBidMatrixCalculationInputPartialBidMatricesAPI",
    "TotalBidMatrixCalculationOutputAPI",
    "TotalBidMatrixCalculationOutputAlertsAPI",
    "TurbineEfficiencyCurveAPI",
    "WaterValueBasedPartialBidConfigurationAPI",
    "WaterValueBasedPartialBidMatrixCalculationInputAPI",
    "WatercourseAPI",
]
