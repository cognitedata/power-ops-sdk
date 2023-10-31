from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = [
    "Transformation",
    "TransformationApply",
    "TransformationList",
    "TransformationApplyList",
    "TransformationFields",
    "TransformationTextFields",
]


TransformationTextFields = Literal["method", "arguments"]
TransformationFields = Literal["method", "arguments", "order"]

_TRANSFORMATION_PROPERTIES_BY_FIELD = {
    "method": "method",
    "arguments": "arguments",
    "order": "order",
}


class Transformation(DomainModel):
    space: str = "cogShop"
    method: Optional[str] = None
    arguments: Optional[str] = None
    order: Optional[int] = None

    def as_apply(self) -> TransformationApply:
        return TransformationApply(
            external_id=self.external_id,
            method=self.method,
            arguments=self.arguments,
            order=self.order,
        )


class TransformationApply(DomainModelApply):
    space: str = "cogShop"
    method: str
    arguments: str
    order: int

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.method is not None:
            properties["method"] = self.method
        if self.arguments is not None:
            properties["arguments"] = self.arguments
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

    def as_apply(self) -> TransformationApplyList:
        return TransformationApplyList([node.as_apply() for node in self.data])


class TransformationApplyList(TypeApplyList[TransformationApply]):
    _NODE = TransformationApply
