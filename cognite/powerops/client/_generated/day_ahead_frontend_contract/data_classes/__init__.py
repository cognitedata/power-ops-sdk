from ._core import (
    DomainModel,
    DomainModelApply,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    ResourcesApplyResult,
)
from ._alert import Alert, AlertApply, AlertApplyList, AlertFields, AlertList, AlertTextFields
from ._bid_document import (
    BidDocument,
    BidDocumentApply,
    BidDocumentApplyList,
    BidDocumentFields,
    BidDocumentList,
    BidDocumentTextFields,
)
from ._bid_method import (
    BidMethod,
    BidMethodApply,
    BidMethodApplyList,
    BidMethodFields,
    BidMethodList,
    BidMethodTextFields,
)
from ._bid_table import BidTable, BidTableApply, BidTableApplyList, BidTableFields, BidTableList, BidTableTextFields
from ._price_area import (
    PriceArea,
    PriceAreaApply,
    PriceAreaApplyList,
    PriceAreaFields,
    PriceAreaList,
    PriceAreaTextFields,
)
from ._production_price_pair import (
    ProductionPricePair,
    ProductionPricePairApply,
    ProductionPricePairApplyList,
    ProductionPricePairFields,
    ProductionPricePairList,
)
from ._shop_multi_scenario import (
    SHOPMultiScenario,
    SHOPMultiScenarioApply,
    SHOPMultiScenarioApplyList,
    SHOPMultiScenarioFields,
    SHOPMultiScenarioList,
    SHOPMultiScenarioTextFields,
)
from ._shop_table import (
    SHOPTable,
    SHOPTableApply,
    SHOPTableApplyList,
    SHOPTableFields,
    SHOPTableList,
    SHOPTableTextFields,
)
from ._water_value_based import (
    WaterValueBased,
    WaterValueBasedApply,
    WaterValueBasedApplyList,
    WaterValueBasedFields,
    WaterValueBasedList,
    WaterValueBasedTextFields,
)

BidDocument.model_rebuild()
BidDocumentApply.model_rebuild()
BidTable.model_rebuild()
BidTableApply.model_rebuild()
PriceArea.model_rebuild()
PriceAreaApply.model_rebuild()
SHOPTable.model_rebuild()
SHOPTableApply.model_rebuild()

__all__ = [
    "ResourcesApply",
    "DomainModel",
    "DomainModelApply",
    "DomainModelList",
    "DomainRelationApply",
    "ResourcesApplyResult",
    "Alert",
    "AlertApply",
    "AlertList",
    "AlertApplyList",
    "AlertFields",
    "AlertTextFields",
    "BidDocument",
    "BidDocumentApply",
    "BidDocumentList",
    "BidDocumentApplyList",
    "BidDocumentFields",
    "BidDocumentTextFields",
    "BidMethod",
    "BidMethodApply",
    "BidMethodList",
    "BidMethodApplyList",
    "BidMethodFields",
    "BidMethodTextFields",
    "BidTable",
    "BidTableApply",
    "BidTableList",
    "BidTableApplyList",
    "BidTableFields",
    "BidTableTextFields",
    "PriceArea",
    "PriceAreaApply",
    "PriceAreaList",
    "PriceAreaApplyList",
    "PriceAreaFields",
    "PriceAreaTextFields",
    "ProductionPricePair",
    "ProductionPricePairApply",
    "ProductionPricePairList",
    "ProductionPricePairApplyList",
    "ProductionPricePairFields",
    "SHOPMultiScenario",
    "SHOPMultiScenarioApply",
    "SHOPMultiScenarioList",
    "SHOPMultiScenarioApplyList",
    "SHOPMultiScenarioFields",
    "SHOPMultiScenarioTextFields",
    "SHOPTable",
    "SHOPTableApply",
    "SHOPTableList",
    "SHOPTableApplyList",
    "SHOPTableFields",
    "SHOPTableTextFields",
    "WaterValueBased",
    "WaterValueBasedApply",
    "WaterValueBasedList",
    "WaterValueBasedApplyList",
    "WaterValueBasedFields",
    "WaterValueBasedTextFields",
]
