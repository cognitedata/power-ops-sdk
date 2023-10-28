from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = [
    "OutputMapping",
    "OutputMappingApply",
    "OutputMappingList",
    "OutputMappingApplyList",
    "OutputMappingFields",
    "OutputMappingTextFields",
]


OutputMappingTextFields = Literal["shop_object_type", "shop_attribute_name", "cdf_attribute_name", "unit"]
OutputMappingFields = Literal["shop_object_type", "shop_attribute_name", "cdf_attribute_name", "unit", "is_step"]

_OUTPUTMAPPING_PROPERTIES_BY_FIELD = {
    "shop_object_type": "shopObjectType",
    "shop_attribute_name": "shopAttributeName",
    "cdf_attribute_name": "cdfAttributeName",
    "unit": "unit",
    "is_step": "isStep",
}


class OutputMapping(DomainModel):
    space: str = "power-ops"
    shop_object_type: Optional[str] = Field(None, alias="shopObjectType")
    shop_attribute_name: Optional[str] = Field(None, alias="shopAttributeName")
    cdf_attribute_name: Optional[str] = Field(None, alias="cdfAttributeName")
    unit: Optional[str] = None
    is_step: Optional[bool] = Field(None, alias="isStep")

    def as_apply(self) -> OutputMappingApply:
        return OutputMappingApply(
            external_id=self.external_id,
            shop_object_type=self.shop_object_type,
            shop_attribute_name=self.shop_attribute_name,
            cdf_attribute_name=self.cdf_attribute_name,
            unit=self.unit,
            is_step=self.is_step,
        )


class OutputMappingApply(DomainModelApply):
    space: str = "power-ops"
    shop_object_type: Optional[str] = Field(None, alias="shopObjectType")
    shop_attribute_name: Optional[str] = Field(None, alias="shopAttributeName")
    cdf_attribute_name: Optional[str] = Field(None, alias="cdfAttributeName")
    unit: Optional[str] = None
    is_step: Optional[bool] = Field(None, alias="isStep")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.shop_object_type is not None:
            properties["shopObjectType"] = self.shop_object_type
        if self.shop_attribute_name is not None:
            properties["shopAttributeName"] = self.shop_attribute_name
        if self.cdf_attribute_name is not None:
            properties["cdfAttributeName"] = self.cdf_attribute_name
        if self.unit is not None:
            properties["unit"] = self.unit
        if self.is_step is not None:
            properties["isStep"] = self.is_step
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

    def as_apply(self) -> OutputMappingApplyList:
        return OutputMappingApplyList([node.as_apply() for node in self.data])


class OutputMappingApplyList(TypeApplyList[OutputMappingApply]):
    _NODE = OutputMappingApply
