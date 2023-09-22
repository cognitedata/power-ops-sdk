from cognite.powerops.client._generated.data_classes._benchmark_bids import (
    BenchmarkBid,
    BenchmarkBidApply,
    BenchmarkBidList,
)
from cognite.powerops.client._generated.data_classes._benchmark_process import (
    BenchmarkProces,
    BenchmarkProcesApply,
    BenchmarkProcesList,
)
from cognite.powerops.client._generated.data_classes._bid_matrix_generators import (
    BidMatrixGenerator,
    BidMatrixGeneratorApply,
    BidMatrixGeneratorList,
)
from cognite.powerops.client._generated.data_classes._command_configs import (
    CommandConfig,
    CommandConfigApply,
    CommandConfigList,
)
from cognite.powerops.client._generated.data_classes._date_transformations import (
    DateTransformation,
    DateTransformationApply,
    DateTransformationList,
)
from cognite.powerops.client._generated.data_classes._day_ahead_bids import (
    DayAheadBid,
    DayAheadBidApply,
    DayAheadBidList,
)
from cognite.powerops.client._generated.data_classes._day_ahead_process import (
    DayAheadProces,
    DayAheadProcesApply,
    DayAheadProcesList,
)
from cognite.powerops.client._generated.data_classes._generators import Generator, GeneratorApply, GeneratorList
from cognite.powerops.client._generated.data_classes._input_time_series_mappings import (
    InputTimeSeriesMapping,
    InputTimeSeriesMappingApply,
    InputTimeSeriesMappingList,
)
from cognite.powerops.client._generated.data_classes._nord_pool_markets import (
    NordPoolMarket,
    NordPoolMarketApply,
    NordPoolMarketList,
)
from cognite.powerops.client._generated.data_classes._output_containers import (
    OutputContainer,
    OutputContainerApply,
    OutputContainerList,
)
from cognite.powerops.client._generated.data_classes._output_mappings import (
    OutputMapping,
    OutputMappingApply,
    OutputMappingList,
)
from cognite.powerops.client._generated.data_classes._plants import Plant, PlantApply, PlantList
from cognite.powerops.client._generated.data_classes._price_areas import PriceArea, PriceAreaApply, PriceAreaList
from cognite.powerops.client._generated.data_classes._production_plan_time_series import (
    ProductionPlanTimeSeries,
    ProductionPlanTimeSeriesApply,
    ProductionPlanTimeSeriesList,
)
from cognite.powerops.client._generated.data_classes._reserve_scenarios import (
    ReserveScenario,
    ReserveScenarioApply,
    ReserveScenarioList,
)
from cognite.powerops.client._generated.data_classes._reservoirs import Reservoir, ReservoirApply, ReservoirList
from cognite.powerops.client._generated.data_classes._rkom_bid_combinations import (
    RKOMBidCombination,
    RKOMBidCombinationApply,
    RKOMBidCombinationList,
)
from cognite.powerops.client._generated.data_classes._rkom_bids import RKOMBid, RKOMBidApply, RKOMBidList
from cognite.powerops.client._generated.data_classes._rkom_combination_bids import (
    RKOMCombinationBid,
    RKOMCombinationBidApply,
    RKOMCombinationBidList,
)
from cognite.powerops.client._generated.data_classes._rkom_markets import RKOMMarket, RKOMMarketApply, RKOMMarketList
from cognite.powerops.client._generated.data_classes._rkom_process import RKOMProces, RKOMProcesApply, RKOMProcesList
from cognite.powerops.client._generated.data_classes._scenario_mappings import (
    ScenarioMapping,
    ScenarioMappingApply,
    ScenarioMappingList,
)
from cognite.powerops.client._generated.data_classes._scenario_templates import (
    ScenarioTemplate,
    ScenarioTemplateApply,
    ScenarioTemplateList,
)
from cognite.powerops.client._generated.data_classes._scenarios import Scenario, ScenarioApply, ScenarioList
from cognite.powerops.client._generated.data_classes._shop_transformations import (
    ShopTransformation,
    ShopTransformationApply,
    ShopTransformationList,
)
from cognite.powerops.client._generated.data_classes._value_transformations import (
    ValueTransformation,
    ValueTransformationApply,
    ValueTransformationList,
)
from cognite.powerops.client._generated.data_classes._watercourse_shops import (
    WatercourseShop,
    WatercourseShopApply,
    WatercourseShopList,
)
from cognite.powerops.client._generated.data_classes._watercourses import Watercourse, WatercourseApply, WatercourseList

BenchmarkBidApply.model_rebuild()
BenchmarkProcesApply.model_rebuild()
DayAheadBidApply.model_rebuild()
DayAheadProcesApply.model_rebuild()
InputTimeSeriesMappingApply.model_rebuild()
OutputContainerApply.model_rebuild()
PlantApply.model_rebuild()
PriceAreaApply.model_rebuild()
RKOMBidApply.model_rebuild()
RKOMBidCombinationApply.model_rebuild()
RKOMProcesApply.model_rebuild()
ScenarioApply.model_rebuild()
ScenarioMappingApply.model_rebuild()
ScenarioTemplateApply.model_rebuild()
ShopTransformationApply.model_rebuild()
WatercourseApply.model_rebuild()

__all__ = [
    "BenchmarkBid",
    "BenchmarkBidApply",
    "BenchmarkBidList",
    "BenchmarkProces",
    "BenchmarkProcesApply",
    "BenchmarkProcesList",
    "BidMatrixGenerator",
    "BidMatrixGeneratorApply",
    "BidMatrixGeneratorList",
    "CommandConfig",
    "CommandConfigApply",
    "CommandConfigList",
    "DateTransformation",
    "DateTransformationApply",
    "DateTransformationList",
    "DayAheadBid",
    "DayAheadBidApply",
    "DayAheadBidList",
    "DayAheadProces",
    "DayAheadProcesApply",
    "DayAheadProcesList",
    "Generator",
    "GeneratorApply",
    "GeneratorList",
    "InputTimeSeriesMapping",
    "InputTimeSeriesMappingApply",
    "InputTimeSeriesMappingList",
    "NordPoolMarket",
    "NordPoolMarketApply",
    "NordPoolMarketList",
    "OutputContainer",
    "OutputContainerApply",
    "OutputContainerList",
    "OutputMapping",
    "OutputMappingApply",
    "OutputMappingList",
    "Plant",
    "PlantApply",
    "PlantList",
    "PriceArea",
    "PriceAreaApply",
    "PriceAreaList",
    "ProductionPlanTimeSeries",
    "ProductionPlanTimeSeriesApply",
    "ProductionPlanTimeSeriesList",
    "RKOMBid",
    "RKOMBidApply",
    "RKOMBidList",
    "RKOMBidCombination",
    "RKOMBidCombinationApply",
    "RKOMBidCombinationList",
    "RKOMCombinationBid",
    "RKOMCombinationBidApply",
    "RKOMCombinationBidList",
    "RKOMMarket",
    "RKOMMarketApply",
    "RKOMMarketList",
    "RKOMProces",
    "RKOMProcesApply",
    "RKOMProcesList",
    "ReserveScenario",
    "ReserveScenarioApply",
    "ReserveScenarioList",
    "Reservoir",
    "ReservoirApply",
    "ReservoirList",
    "Scenario",
    "ScenarioApply",
    "ScenarioList",
    "ScenarioMapping",
    "ScenarioMappingApply",
    "ScenarioMappingList",
    "ScenarioTemplate",
    "ScenarioTemplateApply",
    "ScenarioTemplateList",
    "ShopTransformation",
    "ShopTransformationApply",
    "ShopTransformationList",
    "ValueTransformation",
    "ValueTransformationApply",
    "ValueTransformationList",
    "Watercourse",
    "WatercourseApply",
    "WatercourseList",
    "WatercourseShop",
    "WatercourseShopApply",
    "WatercourseShopList",
]
