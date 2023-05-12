from __future__ import annotations

from cognite.powerops.client._dm_client_base import DMClientBase
from cognite.powerops.client.dm.schema import Transformation


class TransformationClient(DMClientBase[Transformation]):
    model_class = Transformation
    dm_attr = "transformation"
