from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm

from cognite.powerops.clients.data_classes._core import DomainModel, DomainModelApply, InstancesApply, TypeList

__all__ = ["ValueTransformation", "ValueTransformationApply", "ValueTransformationList"]


class ValueTransformation(DomainModel):
    space: ClassVar[str] = "power-ops"
    arguments: Optional[dict] = None
    method: Optional[str] = None


class ValueTransformationApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    arguments: Optional[dict] = None
    method: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "ValueTransformation"),
            properties={
                "arguments": self.arguments,
                "method": self.method,
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


class ValueTransformationList(TypeList[ValueTransformation]):
    _NODE = ValueTransformation
