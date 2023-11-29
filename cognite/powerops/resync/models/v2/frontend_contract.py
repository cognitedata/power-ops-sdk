from __future__ import annotations

from cognite.powerops import PowerOpsClient
from cognite.powerops.resync.models.base import DataModel, T_Model

from . import dms


class FrontendContractModel(DataModel):
    source_model = dms.FrontendContractSourceDMSModel
    dms_model = dms.FrontendContractDMSModel

    @classmethod
    def from_cdf(cls: type[T_Model], client: PowerOpsClient, data_set_external_id: str) -> T_Model:
        raise NotImplementedError()

    def standardize(self) -> None:
        raise NotImplementedError()
