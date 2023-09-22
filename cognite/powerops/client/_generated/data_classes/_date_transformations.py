from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes._core import DomainModel, DomainModelApply, TypeList

__all__ = ["DateTransformation", "DateTransformationApply", "DateTransformationList"]


class DateTransformation(DomainModel):
    space: ClassVar[str] = "power-ops"
    args: list[str] = []
    kwargs: Optional[dict] = None
    transformation: Optional[str] = None


class DateTransformationApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    args: list[str] = []
    kwargs: Optional[dict] = None
    transformation: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.args is not None:
            properties["args"] = self.args
        if self.kwargs is not None:
            properties["kwargs"] = self.kwargs
        if self.transformation is not None:
            properties["transformation"] = self.transformation
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "DateTransformation"),
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


class DateTransformationList(TypeList[DateTransformation]):
    _NODE = DateTransformation
