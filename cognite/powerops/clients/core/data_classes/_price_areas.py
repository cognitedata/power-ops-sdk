from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from ._plants import PlantApply
    from ._watercourses import WatercourseApply

__all__ = ["PriceArea", "PriceAreaApply", "PriceAreaList"]


class PriceArea(DomainModel):
    space: ClassVar[str] = "power-ops"
    day_ahead_price: Optional[str] = Field(None, alias="dayAheadPrice")
    name: Optional[str] = None
    plants: list[str] = []
    watercourses: list[str] = []


class PriceAreaApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    day_ahead_price: Optional[str] = None
    name: Optional[str] = None
    plants: list[Union[str, "PlantApply"]] = Field(default_factory=lambda: [], repr=False)
    watercourses: list[Union[str, "WatercourseApply"]] = Field(default_factory=lambda: [], repr=False)

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "PriceArea"),
            properties={
                "dayAheadPrice": self.day_ahead_price,
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

        for plant in self.plants:
            edge = self._create_plant_edge(plant)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(plant, DomainModelApply):
                instances = plant._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for watercourse in self.watercourses:
            edge = self._create_watercourse_edge(watercourse)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(watercourse, DomainModelApply):
                instances = watercourse._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return InstancesApply(nodes, edges)

    def _create_plant_edge(self, plant: Union[str, "PlantApply"]) -> dm.EdgeApply:
        if isinstance(plant, str):
            end_node_ext_id = plant
        elif isinstance(plant, DomainModelApply):
            end_node_ext_id = plant.external_id
        else:
            raise TypeError(f"Expected str or PlantApply, got {type(plant)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "PriceArea.plants"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )

    def _create_watercourse_edge(self, watercourse: Union[str, "WatercourseApply"]) -> dm.EdgeApply:
        if isinstance(watercourse, str):
            end_node_ext_id = watercourse
        elif isinstance(watercourse, DomainModelApply):
            end_node_ext_id = watercourse.external_id
        else:
            raise TypeError(f"Expected str or WatercourseApply, got {type(watercourse)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "PriceArea.watercourses"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class PriceAreaList(TypeList[PriceArea]):
    _NODE = PriceArea
