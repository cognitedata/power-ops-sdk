from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, InstancesApply, TypeList

__all__ = ["Transformation", "TransformationApply", "TransformationList"]


class Transformation(DomainModel):
    space: ClassVar[str] = "cogShop"
    arguments: Optional[str] = None
    method: Optional[str] = None


class TransformationApply(DomainModelApply):
    space: ClassVar[str] = "cogShop"
    arguments: str
    method: str

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("cogShop", "Transformation"),
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


class TransformationList(TypeList[Transformation]):
    _NODE = Transformation
