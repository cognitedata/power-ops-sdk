from cognite.powerops.resync.models.v1 import ProductionModel
from cognite.powerops.resync.models.v2 import ProductionModelDM


def production_as_asset(dm: ProductionModelDM) -> ProductionModel:
    raise NotImplementedError()
