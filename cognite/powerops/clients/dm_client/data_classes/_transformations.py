from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm

from cognite.powerops.client.dm_client.data_classes._core import (
    CircularModelApply,
    DomainModel,
    InstancesApply,
    TypeList,
)

__all__ = ["Transformation", "TransformationApply", "TransformationList"]


class Transformation(DomainModel):
    space: ClassVar[str] = "cogShop"
    method: Optional[str] = None
    arguments: Optional[str] = None


class TransformationApply(CircularModelApply):
    space: ClassVar[str] = "cogShop"
    method: str
    arguments: str

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=[
                dm.NodeOrEdgeData(
                    source=dm.ContainerId("cogShop", "Transformation"),
                    properties={
                        "method": self.method,
                        "arguments": self.arguments,
                    },
                ),
            ],
        )
        nodes = [this_node]
        edges = []

        return InstancesApply(nodes, edges)


class TransformationList(TypeList[Transformation]):
    _NODE = Transformation
