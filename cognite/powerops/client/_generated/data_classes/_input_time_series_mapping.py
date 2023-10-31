from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._value_transformation import ValueTransformationApply

__all__ = [
    "InputTimeSeriesMapping",
    "InputTimeSeriesMappingApply",
    "InputTimeSeriesMappingList",
    "InputTimeSeriesMappingApplyList",
    "InputTimeSeriesMappingFields",
    "InputTimeSeriesMappingTextFields",
]


InputTimeSeriesMappingTextFields = Literal[
    "shop_object_type", "shop_object_name", "shop_attribute_name", "cdf_time_series", "retrieve", "aggregation"
]
InputTimeSeriesMappingFields = Literal[
    "shop_object_type", "shop_object_name", "shop_attribute_name", "cdf_time_series", "retrieve", "aggregation"
]

_INPUTTIMESERIESMAPPING_PROPERTIES_BY_FIELD = {
    "shop_object_type": "shopObjectType",
    "shop_object_name": "shopObjectName",
    "shop_attribute_name": "shopAttributeName",
    "cdf_time_series": "cdfTimeSeries",
    "retrieve": "retrieve",
    "aggregation": "aggregation",
}


class InputTimeSeriesMapping(DomainModel):
    space: str = "power-ops"
    shop_object_type: Optional[str] = Field(None, alias="shopObjectType")
    shop_object_name: Optional[str] = Field(None, alias="shopObjectName")
    shop_attribute_name: Optional[str] = Field(None, alias="shopAttributeName")
    cdf_time_series: Optional[str] = Field(None, alias="cdfTimeSeries")
    retrieve: Optional[str] = None
    aggregation: Optional[str] = None
    transformations: Optional[list[str]] = None

    def as_apply(self) -> InputTimeSeriesMappingApply:
        return InputTimeSeriesMappingApply(
            external_id=self.external_id,
            shop_object_type=self.shop_object_type,
            shop_object_name=self.shop_object_name,
            shop_attribute_name=self.shop_attribute_name,
            cdf_time_series=self.cdf_time_series,
            retrieve=self.retrieve,
            aggregation=self.aggregation,
            transformations=self.transformations,
        )


class InputTimeSeriesMappingApply(DomainModelApply):
    space: str = "power-ops"
    shop_object_type: Optional[str] = Field(None, alias="shopObjectType")
    shop_object_name: Optional[str] = Field(None, alias="shopObjectName")
    shop_attribute_name: Optional[str] = Field(None, alias="shopAttributeName")
    cdf_time_series: Optional[str] = Field(None, alias="cdfTimeSeries")
    retrieve: Optional[str] = None
    aggregation: Optional[str] = None
    transformations: Union[list[ValueTransformationApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.shop_object_type is not None:
            properties["shopObjectType"] = self.shop_object_type
        if self.shop_object_name is not None:
            properties["shopObjectName"] = self.shop_object_name
        if self.shop_attribute_name is not None:
            properties["shopAttributeName"] = self.shop_attribute_name
        if self.cdf_time_series is not None:
            properties["cdfTimeSeries"] = self.cdf_time_series
        if self.retrieve is not None:
            properties["retrieve"] = self.retrieve
        if self.aggregation is not None:
            properties["aggregation"] = self.aggregation
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

        for transformation in self.transformations or []:
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

    def as_apply(self) -> InputTimeSeriesMappingApplyList:
        return InputTimeSeriesMappingApplyList([node.as_apply() for node in self.data])


class InputTimeSeriesMappingApplyList(TypeApplyList[InputTimeSeriesMappingApply]):
    _NODE = InputTimeSeriesMappingApply
