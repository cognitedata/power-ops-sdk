from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client._generated.data_classes._core import DomainModel, DomainModelApply, TypeList

if TYPE_CHECKING:
    from cognite.powerops.client._generated.data_classes._value_transformations import ValueTransformationApply

__all__ = ["InputTimeSeriesMapping", "InputTimeSeriesMappingApply", "InputTimeSeriesMappingList"]


class InputTimeSeriesMapping(DomainModel):
    space: ClassVar[str] = "power-ops"
    aggregation: Optional[str] = None
    cdf_time_series: Optional[str] = Field(None, alias="cdfTimeSeries")
    retrieve: Optional[str] = None
    shop_attribute_name: Optional[str] = Field(None, alias="shopAttributeName")
    shop_object_name: Optional[str] = Field(None, alias="shopObjectName")
    shop_object_type: Optional[str] = Field(None, alias="shopObjectType")
    transformations: list[str] = []


class InputTimeSeriesMappingApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    aggregation: Optional[str] = None
    cdf_time_series: Optional[str] = None
    retrieve: Optional[str] = None
    shop_attribute_name: Optional[str] = None
    shop_object_name: Optional[str] = None
    shop_object_type: Optional[str] = None
    transformations: list[Union[ValueTransformationApply, str]] = Field(default_factory=list, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.aggregation is not None:
            properties["aggregation"] = self.aggregation
        if self.cdf_time_series is not None:
            properties["cdfTimeSeries"] = self.cdf_time_series
        if self.retrieve is not None:
            properties["retrieve"] = self.retrieve
        if self.shop_attribute_name is not None:
            properties["shopAttributeName"] = self.shop_attribute_name
        if self.shop_object_name is not None:
            properties["shopObjectName"] = self.shop_object_name
        if self.shop_object_type is not None:
            properties["shopObjectType"] = self.shop_object_type
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "InputTimeSeriesMapping"),
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

        for transformation in self.transformations:
            edge = self._create_transformation_edge(transformation)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(transformation, DomainModelApply):
                instances = transformation._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_transformation_edge(self, transformation: Union[str, ValueTransformationApply]) -> dm.EdgeApply:
        if isinstance(transformation, str):
            end_node_ext_id = transformation
        elif isinstance(transformation, DomainModelApply):
            end_node_ext_id = transformation.external_id
        else:
            raise TypeError(f"Expected str or ValueTransformationApply, got {type(transformation)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "InputTimeSeriesMapping.transformations"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class InputTimeSeriesMappingList(TypeList[InputTimeSeriesMapping]):
    _NODE = InputTimeSeriesMapping
