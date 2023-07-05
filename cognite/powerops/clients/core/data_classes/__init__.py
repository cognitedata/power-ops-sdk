from ._generators import Generator, GeneratorApply, GeneratorList
from ._plants import Plant, PlantApply, PlantList
from ._price_areas import PriceArea, PriceAreaApply, PriceAreaList
from ._reservoirs import Reservoir, ReservoirApply, ReservoirList
from ._watercourses import Watercourse, WatercourseApply, WatercourseList

PlantApply.update_forward_refs(
    GeneratorApply=GeneratorApply,
    ReservoirApply=ReservoirApply,
)
PriceAreaApply.update_forward_refs(
    PlantApply=PlantApply,
    WatercourseApply=WatercourseApply,
)
WatercourseApply.update_forward_refs(
    PlantApply=PlantApply,
)

__all__ = [
    "Generator",
    "GeneratorApply",
    "GeneratorList",
    "Plant",
    "PlantApply",
    "PlantList",
    "PriceArea",
    "PriceAreaApply",
    "PriceAreaList",
    "Reservoir",
    "ReservoirApply",
    "ReservoirList",
    "Watercourse",
    "WatercourseApply",
    "WatercourseList",
]
