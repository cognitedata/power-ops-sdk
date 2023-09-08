from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.cogshop1.data_classes._core import DomainModel, DomainModelApply, TypeList

__all__ = ["Transformation", "TransformationApply", "TransformationList"]


class Transformation(DomainModel):
    space: ClassVar[str] = "cogShop"
    arguments: Optional[str] = None
    method: Optional[str] = None
    order: Optional[int] = None


class TransformationApply(DomainModelApply):
    space: ClassVar[str] = "cogShop"
    arguments: str
    method: str
    order: int

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.arguments is not None:
            properties["arguments"] = self.arguments
        if self.method is not None:
            properties["method"] = self.method
        if self.order is not None:
            properties["order"] = self.order
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("cogShop", "Transformation"),
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


class TransformationList(TypeList[Transformation]):
    _NODE = Transformation
