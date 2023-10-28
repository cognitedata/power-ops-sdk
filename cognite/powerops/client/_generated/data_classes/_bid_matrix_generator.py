from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = [
    "BidMatrixGenerator",
    "BidMatrixGeneratorApply",
    "BidMatrixGeneratorList",
    "BidMatrixGeneratorApplyList",
    "BidMatrixGeneratorFields",
    "BidMatrixGeneratorTextFields",
]


BidMatrixGeneratorTextFields = Literal["shop_plant", "methods", "function_external_id"]
BidMatrixGeneratorFields = Literal["shop_plant", "methods", "function_external_id"]

_BIDMATRIXGENERATOR_PROPERTIES_BY_FIELD = {
    "shop_plant": "shopPlant",
    "methods": "methods",
    "function_external_id": "functionExternalId",
}


class BidMatrixGenerator(DomainModel):
    space: str = "power-ops"
    shop_plant: Optional[str] = Field(None, alias="shopPlant")
    methods: Optional[str] = None
    function_external_id: Optional[str] = Field(None, alias="functionExternalId")

    def as_apply(self) -> BidMatrixGeneratorApply:
        return BidMatrixGeneratorApply(
            external_id=self.external_id,
            shop_plant=self.shop_plant,
            methods=self.methods,
            function_external_id=self.function_external_id,
        )


class BidMatrixGeneratorApply(DomainModelApply):
    space: str = "power-ops"
    shop_plant: Optional[str] = Field(None, alias="shopPlant")
    methods: Optional[str] = None
    function_external_id: Optional[str] = Field(None, alias="functionExternalId")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.shop_plant is not None:
            properties["shopPlant"] = self.shop_plant
        if self.methods is not None:
            properties["methods"] = self.methods
        if self.function_external_id is not None:
            properties["functionExternalId"] = self.function_external_id
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

    def as_apply(self) -> BidMatrixGeneratorApplyList:
        return BidMatrixGeneratorApplyList([node.as_apply() for node in self.data])


class BidMatrixGeneratorApplyList(TypeApplyList[BidMatrixGeneratorApply]):
    _NODE = BidMatrixGeneratorApply
