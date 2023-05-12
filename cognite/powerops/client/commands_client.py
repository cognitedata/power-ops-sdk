from __future__ import annotations

from cognite.powerops.client._dm_client_base import DMClientBase
from cognite.powerops.client.dm.schema import CommandsConfig


class CommandsClient(DMClientBase[CommandsConfig]):
    model_class = CommandsConfig
    dm_attr = "commands"
