from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client.dm_client.data_classes._core import (
    CircularModelApply,
    DomainModel,
    InstancesApply,
    TypeList,
)

__all__ = ["ProcessingLog", "ProcessingLogApply", "ProcessingLogList"]


class ProcessingLog(DomainModel):
    space: ClassVar[str] = "cogShop"
    state: Optional[str] = None
    timestamp: Optional[str] = None
    error_message: Optional[str] = Field(None, alias="errorMessage")


class ProcessingLogApply(CircularModelApply):
    space: ClassVar[str] = "cogShop"
    state: Optional[str] = None
    timestamp: Optional[str] = None
    error_message: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=[
                dm.NodeOrEdgeData(
                    source=dm.ContainerId("cogShop", "ProcessingLog"),
                    properties={
                        "state": self.state,
                        "timestamp": self.timestamp,
                        "errorMessage": self.error_message,
                    },
                ),
            ],
        )
        nodes = [this_node]
        edges = []

        return InstancesApply(nodes, edges)


class ProcessingLogList(TypeList[ProcessingLog]):
    _NODE = ProcessingLog
