from cognite.powerops.config import Watercourse
from cognite.powerops.data_classes.core import AssetResourceList
from cognite.powerops.data_classes.generators import Generator
from cognite.powerops.data_classes.plant import Plant
from cognite.powerops.data_classes.price_area import PriceArea
from cognite.powerops.data_classes.reservoirs import Reservoir


class WatercourseList(AssetResourceList):
    _RESOURCE = Watercourse


class PlantList(AssetResourceList):
    _RESOURCE = Plant


class ReservoirList(AssetResourceList):
    _RESOURCE = Reservoir


class GeneratorList(AssetResourceList):
    _RESOURCE = Generator


class PriceAreaList(AssetResourceList):
    _RESOURCE = PriceArea
