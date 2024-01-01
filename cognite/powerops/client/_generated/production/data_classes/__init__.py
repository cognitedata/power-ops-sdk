from ._core import (
    DomainModel,
    DomainModelCore,
    DomainModelApply,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    ResourcesApplyResult,
)
from ._generator import (
    Generator,
    GeneratorApply,
    GeneratorApplyList,
    GeneratorFields,
    GeneratorList,
    GeneratorTextFields,
)
from ._plant import Plant, PlantApply, PlantApplyList, PlantFields, PlantList, PlantTextFields
from ._price_area import (
    PriceArea,
    PriceAreaApply,
    PriceAreaApplyList,
    PriceAreaFields,
    PriceAreaList,
    PriceAreaTextFields,
)
from ._reservoir import (
    Reservoir,
    ReservoirApply,
    ReservoirApplyList,
    ReservoirFields,
    ReservoirList,
    ReservoirTextFields,
)
from ._watercourse import (
    Watercourse,
    WatercourseApply,
    WatercourseApplyList,
    WatercourseFields,
    WatercourseList,
    WatercourseTextFields,
)
from ._watercourse_shop import (
    WatercourseShop,
    WatercourseShopApply,
    WatercourseShopApplyList,
    WatercourseShopFields,
    WatercourseShopList,
)

Plant.model_rebuild()
PlantApply.model_rebuild()
PriceArea.model_rebuild()
PriceAreaApply.model_rebuild()
Watercourse.model_rebuild()
WatercourseApply.model_rebuild()

__all__ = [
    "ResourcesApply",
    "DomainModel",
    "DomainModelCore",
    "DomainModelApply",
    "DomainModelList",
    "DomainRelationApply",
    "ResourcesApplyResult",
    "Generator",
    "GeneratorApply",
    "GeneratorList",
    "GeneratorApplyList",
    "GeneratorFields",
    "GeneratorTextFields",
    "Plant",
    "PlantApply",
    "PlantList",
    "PlantApplyList",
    "PlantFields",
    "PlantTextFields",
    "PriceArea",
    "PriceAreaApply",
    "PriceAreaList",
    "PriceAreaApplyList",
    "PriceAreaFields",
    "PriceAreaTextFields",
    "Reservoir",
    "ReservoirApply",
    "ReservoirList",
    "ReservoirApplyList",
    "ReservoirFields",
    "ReservoirTextFields",
    "Watercourse",
    "WatercourseApply",
    "WatercourseList",
    "WatercourseApplyList",
    "WatercourseFields",
    "WatercourseTextFields",
    "WatercourseShop",
    "WatercourseShopApply",
    "WatercourseShopList",
    "WatercourseShopApplyList",
    "WatercourseShopFields",
]
