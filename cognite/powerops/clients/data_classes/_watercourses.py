from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.clients.data_classes._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from cognite.powerops.clients.data_classes._plants import PlantApply
    from cognite.powerops.clients.data_classes._watercourse_shops import WatercourseShopApply

__all__ = ["Watercourse", "WatercourseApply", "WatercourseList"]


class Watercourse(DomainModel):
    space: ClassVar[str] = "power-ops"
    name: Optional[str] = None
    plants: list[str] = []
    production_obligation: list[str] = Field([], alias="productionObligation")
    shop: Optional[str] = None


class WatercourseApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    name: Optional[str] = None
    plants: list[Union["PlantApply", str]] = Field(default_factory=list, repr=False)
    production_obligation: list[str] = []
    shop: Optional[Union["WatercourseShopApply", str]] = Field(None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "Watercourse"),
            properties={
                "name": self.name,
                "productionObligation": self.production_obligation,
                "shop": {
                    "space": "power-ops",
                    "externalId": self.shop if isinstance(self.shop, str) else self.shop.external_id,
                },
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

        if isinstance(self.shop, DomainModelApply):
            instances = self.shop._to_instances_apply(cache)
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
            type=dm.DirectRelationReference("power-ops", "Watercourse.plants"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class WatercourseList(TypeList[Watercourse]):
    _NODE = Watercourse
