from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm

from cognite.powerops.clients.data_classes._core import DomainModel, DomainModelApply, TypeList

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
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "DateTransformation"),
            properties={
                "args": self.args,
                "kwargs": self.kwargs,
                "transformation": self.transformation,
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
        cache.add(self.external_id)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class DateTransformationList(TypeList[DateTransformation]):
    _NODE = DateTransformation
