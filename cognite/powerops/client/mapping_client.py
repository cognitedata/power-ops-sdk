from __future__ import annotations

from cognite.powerops.client._dm_client_base import DMClientBase
from cognite.powerops.client.dm.schema import Mapping


class MappingClient(DMClientBase[Mapping]):
    model_class = Mapping
    dm_attr = "mapping"
