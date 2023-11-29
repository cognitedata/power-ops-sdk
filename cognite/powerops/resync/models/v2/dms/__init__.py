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


FrontendContractSourceDMSModel = PowerOpsDMSSourceModel(
    container_file=_DMS_DIR / "frontendContract" / "containers.yaml",
)

FrontendContractDMSModel = PowerOpsDMSModel(
    name="FrontendContract",
    description="Stores common exported data from different markets, for consumption by the PowerOps UI app.",
    id_=DataModelId("poweropsFrontendContractModel", "FrontendContract", "1"),
    view_file=_DMS_DIR / "frontendContract" / "views.yaml",
)


DayAheadFrontendContractSourceDMSModel = PowerOpsDMSSourceModel(
    container_file=_DMS_DIR / "dayAheadFrontendContract" / "containers.yaml",
)

DayAheadFrontendContractDMSModel = PowerOpsDMSModel(
    name="DayAheadFrontendContract",
    description="Stores exported data for consumption by the PowerOps UI app.",
    id_=DataModelId("poweropsDayAheadFrontendContractModel", "DayAheadFrontendContract", "1"),
    view_file=_DMS_DIR / "dayAheadFrontendContract" / "views.yaml",
)
