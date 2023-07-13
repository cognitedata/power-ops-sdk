from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, InstancesApply, TypeList

__all__ = ["Proces", "ProcesApply", "ProcesList"]


class Proces(DomainModel):
    space: ClassVar[str] = "power-ops"
    name: Optional[str] = None


class ProcesApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    name: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "Process"),
            properties={
                "name": self.name,
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


class ProcesList(TypeList[Proces]):
    _NODE = Proces
