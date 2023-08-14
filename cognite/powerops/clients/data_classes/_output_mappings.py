from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.clients.data_classes._core import DomainModel, DomainModelApply, TypeList

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
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "OutputMapping"),
            properties={
                "cdfAttributeName": self.cdf_attribute_name,
                "isStep": self.is_step,
                "shopAttributeName": self.shop_attribute_name,
                "shopObjectType": self.shop_object_type,
                "unit": self.unit,
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
        cache.add(self.external_id)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class OutputMappingList(TypeList[OutputMapping]):
    _NODE = OutputMapping
