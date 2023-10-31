from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = [
    "Reservoir",
    "ReservoirApply",
    "ReservoirList",
    "ReservoirApplyList",
    "ReservoirFields",
    "ReservoirTextFields",
]


ReservoirTextFields = Literal["name", "display_name"]
ReservoirFields = Literal["name", "display_name", "ordering"]

_RESERVOIR_PROPERTIES_BY_FIELD = {
    "name": "name",
    "display_name": "displayName",
    "ordering": "ordering",
}


class Reservoir(DomainModel):
    space: str = "power-ops"
    name: Optional[str] = None
    display_name: Optional[str] = Field(None, alias="displayName")
    ordering: Optional[int] = None

    def as_apply(self) -> ReservoirApply:
        return ReservoirApply(
            external_id=self.external_id,
            name=self.name,
            display_name=self.display_name,
            ordering=self.ordering,
        )


class ReservoirApply(DomainModelApply):
    space: str = "power-ops"
    name: Optional[str] = None
    display_name: Optional[str] = Field(None, alias="displayName")
    ordering: Optional[int] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.display_name is not None:
            properties["displayName"] = self.display_name
        if self.ordering is not None:
            properties["ordering"] = self.ordering
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "Reservoir"),
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


class ReservoirList(TypeList[Reservoir]):
    _NODE = Reservoir

    def as_apply(self) -> ReservoirApplyList:
        return ReservoirApplyList([node.as_apply() for node in self.data])


class ReservoirApplyList(TypeApplyList[ReservoirApply]):
    _NODE = ReservoirApply
