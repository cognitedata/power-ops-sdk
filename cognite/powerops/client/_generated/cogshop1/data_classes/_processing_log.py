from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = [
    "ProcessingLog",
    "ProcessingLogApply",
    "ProcessingLogList",
    "ProcessingLogApplyList",
    "ProcessingLogFields",
    "ProcessingLogTextFields",
]


ProcessingLogTextFields = Literal["state", "timestamp", "error_message"]
ProcessingLogFields = Literal["state", "timestamp", "error_message"]

_PROCESSINGLOG_PROPERTIES_BY_FIELD = {
    "state": "state",
    "timestamp": "timestamp",
    "error_message": "errorMessage",
}


class ProcessingLog(DomainModel):
    space: str = "cogShop"
    state: Optional[str] = None
    timestamp: Optional[str] = None
    error_message: Optional[str] = Field(None, alias="errorMessage")

    def as_apply(self) -> ProcessingLogApply:
        return ProcessingLogApply(
            external_id=self.external_id,
            state=self.state,
            timestamp=self.timestamp,
            error_message=self.error_message,
        )


class ProcessingLogApply(DomainModelApply):
    space: str = "cogShop"
    state: Optional[str] = None
    timestamp: Optional[str] = None
    error_message: Optional[str] = Field(None, alias="errorMessage")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.state is not None:
            properties["state"] = self.state
        if self.timestamp is not None:
            properties["timestamp"] = self.timestamp
        if self.error_message is not None:
            properties["errorMessage"] = self.error_message
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("cogShop", "ProcessingLog"),
                properties=properties,
            )
            sources.append(source)
        if sources:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=sources,
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class ProcessingLogList(TypeList[ProcessingLog]):
    _NODE = ProcessingLog

    def as_apply(self) -> ProcessingLogApplyList:
        return ProcessingLogApplyList([node.as_apply() for node in self.data])


class ProcessingLogApplyList(TypeApplyList[ProcessingLogApply]):
    _NODE = ProcessingLogApply
