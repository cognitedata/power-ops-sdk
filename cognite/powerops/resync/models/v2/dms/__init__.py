from pathlib import Path

from cognite.client.data_classes.data_modeling import DataModelId

from cognite.powerops.resync.models.base.dms_models import PowerOpsDMSModel, PowerOpsDMSSourceModel

_DMS_DIR = Path(__file__).parent


CapacitySourceModel = PowerOpsDMSSourceModel(
    container_file=_DMS_DIR / "capacityBid" / "containers.yaml",
)

CapacityModel = PowerOpsDMSModel(
    name="CapacityBid",
    description="The CapacityBid model describes the capacity bids for the aFRR and mFRR markets.",
    id_=DataModelId("power-ops", "capacityBid", "1"),
    view_file=_DMS_DIR / "capacityBid" / "views.yaml",
)
