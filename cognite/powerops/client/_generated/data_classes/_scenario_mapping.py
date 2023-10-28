from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._input_time_series_mapping import InputTimeSeriesMappingApply

__all__ = [
    "ScenarioMapping",
    "ScenarioMappingApply",
    "ScenarioMappingList",
    "ScenarioMappingApplyList",
    "ScenarioMappingFields",
    "ScenarioMappingTextFields",
]


ScenarioMappingTextFields = Literal["name", "watercourse", "shop_type"]
ScenarioMappingFields = Literal["name", "watercourse", "shop_type"]

_SCENARIOMAPPING_PROPERTIES_BY_FIELD = {
    "name": "name",
    "watercourse": "watercourse",
    "shop_type": "shopType",
}


class ScenarioMapping(DomainModel):
    space: str = "power-ops"
    name: Optional[str] = None
    watercourse: Optional[str] = None
    shop_type: Optional[str] = Field(None, alias="shopType")
    mapping_override: Optional[list[str]] = Field(None, alias="mappingOverride")

    def as_apply(self) -> ScenarioMappingApply:
        return ScenarioMappingApply(
            external_id=self.external_id,
            name=self.name,
            watercourse=self.watercourse,
            shop_type=self.shop_type,
            mapping_override=self.mapping_override,
        )


class ScenarioMappingApply(DomainModelApply):
    space: str = "power-ops"
    name: Optional[str] = None
    watercourse: Optional[str] = None
    shop_type: Optional[str] = Field(None, alias="shopType")
    mapping_override: Union[list[InputTimeSeriesMappingApply], list[str], None] = Field(
        default=None, repr=False, alias="mappingOverride"
    )

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.watercourse is not None:
            properties["watercourse"] = self.watercourse
        if self.shop_type is not None:
            properties["shopType"] = self.shop_type
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "ScenarioMapping"),
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

        for mapping_override in self.mapping_override or []:
            edge = self._create_mapping_override_edge(mapping_override)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(mapping_override, DomainModelApply):
                instances = mapping_override._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_mapping_override_edge(self, mapping_override: Union[str, InputTimeSeriesMappingApply]) -> dm.EdgeApply:
        if isinstance(mapping_override, str):
            end_node_ext_id = mapping_override
        elif isinstance(mapping_override, DomainModelApply):
            end_node_ext_id = mapping_override.external_id
        else:
            raise TypeError(f"Expected str or InputTimeSeriesMappingApply, got {type(mapping_override)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "ScenarioMapping.mappingOverride"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class ScenarioMappingList(TypeList[ScenarioMapping]):
    _NODE = ScenarioMapping

    def as_apply(self) -> ScenarioMappingApplyList:
        return ScenarioMappingApplyList([node.as_apply() for node in self.data])


class ScenarioMappingApplyList(TypeApplyList[ScenarioMappingApply]):
    _NODE = ScenarioMappingApply
