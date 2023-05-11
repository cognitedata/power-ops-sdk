from cognite.powerops.config import Watercourse
from cognite.powerops.data_classes.core import AssetResourceList
from cognite.powerops.data_classes.plant import Plant


class WatercourseList(AssetResourceList):
    _RESOURCE = Watercourse


class PlantList(AssetResourceList):
    _RESOURCE = Plant
