from ._core import (
    DomainModel,
    DomainModelCore,
    DomainModelApply,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    ResourcesApplyResult,
)
from ._alert import Alert, AlertApply, AlertApplyList, AlertFields, AlertList, AlertTextFields
from ._basic_bid_matrix import (
    BasicBidMatrix,
    BasicBidMatrixApply,
    BasicBidMatrixApplyList,
    BasicBidMatrixFields,
    BasicBidMatrixList,
    BasicBidMatrixTextFields,
)
from ._bid_document import (
    BidDocument,
    BidDocumentApply,
    BidDocumentApplyList,
    BidDocumentFields,
    BidDocumentList,
    BidDocumentTextFields,
)
from ._bid_matrix import (
    BidMatrix,
    BidMatrixApply,
    BidMatrixApplyList,
    BidMatrixFields,
    BidMatrixList,
    BidMatrixTextFields,
)
from ._bid_method import (
    BidMethod,
    BidMethodApply,
    BidMethodApplyList,
    BidMethodFields,
    BidMethodList,
    BidMethodTextFields,
)
from ._custom_bid_matrix import (
    CustomBidMatrix,
    CustomBidMatrixApply,
    CustomBidMatrixApplyList,
    CustomBidMatrixFields,
    CustomBidMatrixList,
    CustomBidMatrixTextFields,
)
from ._custom_bid_method import (
    CustomBidMethod,
    CustomBidMethodApply,
    CustomBidMethodApplyList,
    CustomBidMethodFields,
    CustomBidMethodList,
    CustomBidMethodTextFields,
)
from ._multi_scenario_matrix import (
    MultiScenarioMatrix,
    MultiScenarioMatrixApply,
    MultiScenarioMatrixApplyList,
    MultiScenarioMatrixFields,
    MultiScenarioMatrixList,
    MultiScenarioMatrixTextFields,
)
from ._price_area import (
    PriceArea,
    PriceAreaApply,
    PriceAreaApplyList,
    PriceAreaFields,
    PriceAreaList,
    PriceAreaTextFields,
)
from ._shop_multi_scenario import (
    SHOPMultiScenario,
    SHOPMultiScenarioApply,
    SHOPMultiScenarioApplyList,
    SHOPMultiScenarioFields,
    SHOPMultiScenarioList,
    SHOPMultiScenarioTextFields,
)
from ._water_value_based import (
    WaterValueBased,
    WaterValueBasedApply,
    WaterValueBasedApplyList,
    WaterValueBasedFields,
    WaterValueBasedList,
    WaterValueBasedTextFields,
)

BasicBidMatrix.model_rebuild()
BasicBidMatrixApply.model_rebuild()
BidDocument.model_rebuild()
BidDocumentApply.model_rebuild()
BidMatrix.model_rebuild()
BidMatrixApply.model_rebuild()
CustomBidMatrix.model_rebuild()
CustomBidMatrixApply.model_rebuild()
MultiScenarioMatrix.model_rebuild()
MultiScenarioMatrixApply.model_rebuild()
PriceArea.model_rebuild()
PriceAreaApply.model_rebuild()

__all__ = [
    "ResourcesApply",
    "DomainModel",
    "DomainModelCore",
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
    "BasicBidMatrix",
    "BasicBidMatrixApply",
    "BasicBidMatrixList",
    "BasicBidMatrixApplyList",
    "BasicBidMatrixFields",
    "BasicBidMatrixTextFields",
    "BidDocument",
    "BidDocumentApply",
    "BidDocumentList",
    "BidDocumentApplyList",
    "BidDocumentFields",
    "BidDocumentTextFields",
    "BidMatrix",
    "BidMatrixApply",
    "BidMatrixList",
    "BidMatrixApplyList",
    "BidMatrixFields",
    "BidMatrixTextFields",
    "BidMethod",
    "BidMethodApply",
    "BidMethodList",
    "BidMethodApplyList",
    "BidMethodFields",
    "BidMethodTextFields",
    "CustomBidMatrix",
    "CustomBidMatrixApply",
    "CustomBidMatrixList",
    "CustomBidMatrixApplyList",
    "CustomBidMatrixFields",
    "CustomBidMatrixTextFields",
    "CustomBidMethod",
    "CustomBidMethodApply",
    "CustomBidMethodList",
    "CustomBidMethodApplyList",
    "CustomBidMethodFields",
    "CustomBidMethodTextFields",
    "MultiScenarioMatrix",
    "MultiScenarioMatrixApply",
    "MultiScenarioMatrixList",
    "MultiScenarioMatrixApplyList",
    "MultiScenarioMatrixFields",
    "MultiScenarioMatrixTextFields",
    "PriceArea",
    "PriceAreaApply",
    "PriceAreaList",
    "PriceAreaApplyList",
    "PriceAreaFields",
    "PriceAreaTextFields",
    "SHOPMultiScenario",
    "SHOPMultiScenarioApply",
    "SHOPMultiScenarioList",
    "SHOPMultiScenarioApplyList",
    "SHOPMultiScenarioFields",
    "SHOPMultiScenarioTextFields",
    "WaterValueBased",
    "WaterValueBasedApply",
    "WaterValueBasedList",
    "WaterValueBasedApplyList",
    "WaterValueBasedFields",
    "WaterValueBasedTextFields",
]
