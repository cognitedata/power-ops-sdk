from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client._generated.data_classes._core import DomainModel, DomainModelApply, TypeList

__all__ = ["BidMatrixGenerator", "BidMatrixGeneratorApply", "BidMatrixGeneratorList"]


class BidMatrixGenerator(DomainModel):
    space: ClassVar[str] = "power-ops"
    function_external_id: Optional[str] = Field(None, alias="functionExternalId")
    methods: Optional[str] = None
    shop_plant: Optional[str] = Field(None, alias="shopPlant")


class BidMatrixGeneratorApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    function_external_id: Optional[str] = None
    methods: Optional[str] = None
    shop_plant: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.function_external_id is not None:
            properties["functionExternalId"] = self.function_external_id
        if self.methods is not None:
            properties["methods"] = self.methods
        if self.shop_plant is not None:
            properties["shopPlant"] = self.shop_plant
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "BidMatrixGenerator"),
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


class BidMatrixGeneratorList(TypeList[BidMatrixGenerator]):
    _NODE = BidMatrixGenerator
