from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client._generated.data_classes._core import DomainModel, DomainModelApply, TypeList

__all__ = ["OutputMapping", "OutputMappingApply", "OutputMappingList"]


class OutputMapping(DomainModel):
    space: ClassVar[str] = "power-ops"
    cdf_attribute_name: Optional[str] = Field(None, alias="cdfAttributeName")
    is_step: Optional[bool] = Field(None, alias="isStep")
    shop_attribute_name: Optional[str] = Field(None, alias="shopAttributeName")
    shop_object_type: Optional[str] = Field(None, alias="shopObjectType")
    unit: Optional[str] = None


class OutputMappingApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    cdf_attribute_name: Optional[str] = None
    is_step: Optional[bool] = None
    shop_attribute_name: Optional[str] = None
    shop_object_type: Optional[str] = None
    unit: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.cdf_attribute_name is not None:
            properties["cdfAttributeName"] = self.cdf_attribute_name
        if self.is_step is not None:
            properties["isStep"] = self.is_step
        if self.shop_attribute_name is not None:
            properties["shopAttributeName"] = self.shop_attribute_name
        if self.shop_object_type is not None:
            properties["shopObjectType"] = self.shop_object_type
        if self.unit is not None:
            properties["unit"] = self.unit
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "OutputMapping"),
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


class OutputMappingList(TypeList[OutputMapping]):
    _NODE = OutputMapping
