from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client._generated.data_classes._core import DomainModel, DomainModelApply, TypeList

__all__ = ["Reservoir", "ReservoirApply", "ReservoirList"]


class Reservoir(DomainModel):
    space: ClassVar[str] = "power-ops"
    display_name: Optional[str] = Field(None, alias="displayName")
    name: Optional[str] = None
    ordering: Optional[int] = None


class ReservoirApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    display_name: Optional[str] = None
    name: Optional[str] = None
    ordering: Optional[int] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.display_name is not None:
            properties["displayName"] = self.display_name
        if self.name is not None:
            properties["name"] = self.name
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
