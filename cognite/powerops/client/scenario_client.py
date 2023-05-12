from __future__ import annotations

from cognite.powerops.client._dm_client_base import DMClientBase
from cognite.powerops.client.dm.schema import Scenario


class ScenarioClient(DMClientBase[Scenario]):
    model_class = Scenario
    dm_attr = "scenario"
