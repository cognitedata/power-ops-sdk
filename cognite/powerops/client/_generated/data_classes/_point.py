from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = ["Point", "PointApply", "PointList", "PointApplyList", "PointFields"]
PointFields = Literal["position", "quantity", "minimum_quantity", "price_amount", "energy_price_amount"]

_POINT_PROPERTIES_BY_FIELD = {
    "position": "position",
    "quantity": "quantity",
    "minimum_quantity": "minimumQuantity",
    "price_amount": "priceAmount",
    "energy_price_amount": "energyPriceAmount",
}


class Point(DomainModel):
    space: str = "power-ops"
    position: Optional[int] = None
    quantity: Optional[float] = None
    minimum_quantity: Optional[float] = Field(None, alias="minimumQuantity")
    price_amount: Optional[float] = Field(None, alias="priceAmount")
    energy_price_amount: Optional[float] = Field(None, alias="energyPriceAmount")

    def as_apply(self) -> PointApply:
        return PointApply(
            external_id=self.external_id,
            position=self.position,
            quantity=self.quantity,
            minimum_quantity=self.minimum_quantity,
            price_amount=self.price_amount,
            energy_price_amount=self.energy_price_amount,
        )


class PointApply(DomainModelApply):
    space: str = "power-ops"
    position: Optional[int] = None
    quantity: Optional[float] = None
    minimum_quantity: Optional[float] = Field(None, alias="minimumQuantity")
    price_amount: Optional[float] = Field(None, alias="priceAmount")
    energy_price_amount: Optional[float] = Field(None, alias="energyPriceAmount")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.position is not None:
            properties["position"] = self.position
        if self.quantity is not None:
            properties["quantity"] = self.quantity
        if self.minimum_quantity is not None:
            properties["minimumQuantity"] = self.minimum_quantity
        if self.price_amount is not None:
            properties["priceAmount"] = self.price_amount
        if self.energy_price_amount is not None:
            properties["energyPriceAmount"] = self.energy_price_amount
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "Point"),
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


class PointList(TypeList[Point]):
    _NODE = Point

    def as_apply(self) -> PointApplyList:
        return PointApplyList([node.as_apply() for node in self.data])


class PointApplyList(TypeApplyList[PointApply]):
    _NODE = PointApply
