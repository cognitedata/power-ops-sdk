from __future__ import annotations

from cognite.powerops.client._dm_client_base import DMClientBase
from cognite.powerops.client.dm.schema import Case


class CaseClient(DMClientBase[Case]):
    model_class = Case
    dm_attr = "case"
