from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = [
    "NordPoolMarket",
    "NordPoolMarketApply",
    "NordPoolMarketList",
    "NordPoolMarketApplyList",
    "NordPoolMarketFields",
    "NordPoolMarketTextFields",
]


NordPoolMarketTextFields = Literal["name", "timezone", "price_unit", "time_unit"]
NordPoolMarketFields = Literal[
    "name", "timezone", "max_price", "min_price", "price_steps", "price_unit", "tick_size", "time_unit", "trade_lot"
]

_NORDPOOLMARKET_PROPERTIES_BY_FIELD = {
    "name": "name",
    "timezone": "timezone",
    "max_price": "maxPrice",
    "min_price": "minPrice",
    "price_steps": "priceSteps",
    "price_unit": "priceUnit",
    "tick_size": "tickSize",
    "time_unit": "timeUnit",
    "trade_lot": "tradeLot",
}


class NordPoolMarket(DomainModel):
    space: str = "power-ops"
    name: Optional[str] = None
    timezone: Optional[str] = None
    max_price: Optional[float] = Field(None, alias="maxPrice")
    min_price: Optional[float] = Field(None, alias="minPrice")
    price_steps: Optional[int] = Field(None, alias="priceSteps")
    price_unit: Optional[str] = Field(None, alias="priceUnit")
    tick_size: Optional[float] = Field(None, alias="tickSize")
    time_unit: Optional[str] = Field(None, alias="timeUnit")
    trade_lot: Optional[float] = Field(None, alias="tradeLot")

    def as_apply(self) -> NordPoolMarketApply:
        return NordPoolMarketApply(
            external_id=self.external_id,
            name=self.name,
            timezone=self.timezone,
            max_price=self.max_price,
            min_price=self.min_price,
            price_steps=self.price_steps,
            price_unit=self.price_unit,
            tick_size=self.tick_size,
            time_unit=self.time_unit,
            trade_lot=self.trade_lot,
        )


class NordPoolMarketApply(DomainModelApply):
    space: str = "power-ops"
    name: Optional[str] = None
    timezone: Optional[str] = None
    max_price: Optional[float] = Field(None, alias="maxPrice")
    min_price: Optional[float] = Field(None, alias="minPrice")
    price_steps: Optional[int] = Field(None, alias="priceSteps")
    price_unit: Optional[str] = Field(None, alias="priceUnit")
    tick_size: Optional[float] = Field(None, alias="tickSize")
    time_unit: Optional[str] = Field(None, alias="timeUnit")
    trade_lot: Optional[float] = Field(None, alias="tradeLot")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.timezone is not None:
            properties["timezone"] = self.timezone
        if self.max_price is not None:
            properties["maxPrice"] = self.max_price
        if self.min_price is not None:
            properties["minPrice"] = self.min_price
        if self.price_steps is not None:
            properties["priceSteps"] = self.price_steps
        if self.price_unit is not None:
            properties["priceUnit"] = self.price_unit
        if self.tick_size is not None:
            properties["tickSize"] = self.tick_size
        if self.time_unit is not None:
            properties["timeUnit"] = self.time_unit
        if self.trade_lot is not None:
            properties["tradeLot"] = self.trade_lot
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "NordPoolMarket"),
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


class NordPoolMarketList(TypeList[NordPoolMarket]):
    _NODE = NordPoolMarket

    def as_apply(self) -> NordPoolMarketApplyList:
        return NordPoolMarketApplyList([node.as_apply() for node in self.data])


class NordPoolMarketApplyList(TypeApplyList[NordPoolMarketApply]):
    _NODE = NordPoolMarketApply
