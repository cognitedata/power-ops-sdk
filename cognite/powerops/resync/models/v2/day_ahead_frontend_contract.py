from __future__ import annotations

from cognite.powerops import PowerOpsClient
from cognite.powerops.resync.models.base import DataModel, T_Model

from . import dms


class DayAheadFrontendContractModel(DataModel):
    source_model = dms.DayAheadFrontendContractSourceDMSModel
    dms_model = dms.DayAheadFrontendContractDMSModel

    @classmethod
    def from_cdf(cls: type[T_Model], client: PowerOpsClient, data_set_external_id: str) -> T_Model:
        raise NotImplementedError()

    def standardize(self) -> None:
        raise NotImplementedError()
