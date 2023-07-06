from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from ._date_transformations import DateTransformationApply

__all__ = ["ShopTransformation", "ShopTransformationApply", "ShopTransformationList"]


class ShopTransformation(DomainModel):
    space: ClassVar[str] = "power-ops"
    end: list[str] = []
    start: list[str] = []


class ShopTransformationApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    end: list[Union[str, "DateTransformationApply"]] = Field(default_factory=lambda: [], repr=False)
    start: list[Union[str, "DateTransformationApply"]] = Field(default_factory=lambda: [], repr=False)

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=sources,
        )
        nodes = [this_node]
        edges = []

        for end in self.end:
            edge = self._create_end_edge(end)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(end, DomainModelApply):
                instances = end._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for start in self.start:
            edge = self._create_start_edge(start)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(start, DomainModelApply):
                instances = start._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return InstancesApply(nodes, edges)

    def _create_end_edge(self, end: Union[str, "DateTransformationApply"]) -> dm.EdgeApply:
        if isinstance(end, str):
            end_node_ext_id = end
        elif isinstance(end, DomainModelApply):
            end_node_ext_id = end.external_id
        else:
            raise TypeError(f"Expected str or DateTransformationApply, got {type(end)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "ShopTransformation.end"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )

    def _create_start_edge(self, start: Union[str, "DateTransformationApply"]) -> dm.EdgeApply:
        if isinstance(start, str):
            end_node_ext_id = start
        elif isinstance(start, DomainModelApply):
            end_node_ext_id = start.external_id
        else:
            raise TypeError(f"Expected str or DateTransformationApply, got {type(start)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "ShopTransformation.start"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class ShopTransformationList(TypeList[ShopTransformation]):
    _NODE = ShopTransformation
