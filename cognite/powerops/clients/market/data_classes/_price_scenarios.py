from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from ._value_transformations import ValueTransformationApply

__all__ = ["PriceScenario", "PriceScenarioApply", "PriceScenarioList"]


class PriceScenario(DomainModel):
    space: ClassVar[str] = "power-ops"
    name: Optional[str] = None
    time_series: Optional[str] = Field(None, alias="timeSeries")
    transformations: list[str] = []


class PriceScenarioApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    name: Optional[str] = None
    time_series: Optional[str] = None
    transformations: list[Union[str, "ValueTransformationApply"]] = Field(default_factory=lambda: [], repr=False)

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "PriceScenario"),
            properties={
                "name": self.name,
                "timeSeries": self.time_series,
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

        for transformation in self.transformations:
            edge = self._create_transformation_edge(transformation)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(transformation, DomainModelApply):
                instances = transformation._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return InstancesApply(nodes, edges)

    def _create_transformation_edge(self, transformation: Union[str, "ValueTransformationApply"]) -> dm.EdgeApply:
        if isinstance(transformation, str):
            end_node_ext_id = transformation
        elif isinstance(transformation, DomainModelApply):
            end_node_ext_id = transformation.external_id
        else:
            raise TypeError(f"Expected str or ValueTransformationApply, got {type(transformation)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "PriceScenario.transformations"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class PriceScenarioList(TypeList[PriceScenario]):
    _NODE = PriceScenario
