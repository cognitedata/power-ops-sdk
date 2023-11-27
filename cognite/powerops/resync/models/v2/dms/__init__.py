from pathlib import Path

import yaml
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


_day_ahead_frontend_contract = yaml.safe_load(
    _DMS_DIR.joinpath(
        "dayAheadFrontendContract", "local_modules", "day_ahead_frontend_contract", "default.config.yaml"
    ).read_text()
)

DayAheadFrontendContractModel = PowerOpsDMSModel(
    name=_day_ahead_frontend_contract["data_model"],
    description=_day_ahead_frontend_contract["data_model_description"],
    id_=DataModelId(
        _day_ahead_frontend_contract["model_space"],
        _day_ahead_frontend_contract["data_model"],
        _day_ahead_frontend_contract["data_model_version"],
    ),
    view_file=_DMS_DIR / "dayAheadFrontendContract" / "views.yaml",  # not true!
)
