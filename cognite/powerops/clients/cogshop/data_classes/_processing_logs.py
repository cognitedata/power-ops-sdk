from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, InstancesApply, TypeList

__all__ = ["ProcessingLog", "ProcessingLogApply", "ProcessingLogList"]


class ProcessingLog(DomainModel):
    space: ClassVar[str] = "cogShop"
    error_message: Optional[str] = Field(None, alias="errorMessage")
    state: Optional[str] = None
    timestamp: Optional[str] = None


class ProcessingLogApply(DomainModelApply):
    space: ClassVar[str] = "cogShop"
    error_message: Optional[str] = None
    state: Optional[str] = None
    timestamp: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("cogShop", "ProcessingLog"),
            properties={
                "errorMessage": self.error_message,
                "state": self.state,
                "timestamp": self.timestamp,
            },
        )
        sources.append(source)

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=sources,
        )
        nodes = [this_node]
        edges = []

        return InstancesApply(nodes, edges)


class ProcessingLogList(TypeList[ProcessingLog]):
    _NODE = ProcessingLog
