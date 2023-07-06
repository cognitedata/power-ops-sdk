from ._generators import Generator, GeneratorApply, GeneratorList
from ._plants import Plant, PlantApply, PlantList
from ._price_areas import PriceArea, PriceAreaApply, PriceAreaList
from ._reservoirs import Reservoir, ReservoirApply, ReservoirList
from ._watercourses import Watercourse, WatercourseApply, WatercourseList

PlantApply.model_rebuild()
PriceAreaApply.model_rebuild()
WatercourseApply.model_rebuild()

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
