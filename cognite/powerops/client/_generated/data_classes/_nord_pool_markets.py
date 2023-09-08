from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client._generated.data_classes._core import DomainModel, DomainModelApply, TypeList

__all__ = ["NordPoolMarket", "NordPoolMarketApply", "NordPoolMarketList"]


class NordPoolMarket(DomainModel):
    space: ClassVar[str] = "power-ops"
    max_price: Optional[float] = Field(None, alias="maxPrice")
    min_price: Optional[float] = Field(None, alias="minPrice")
    name: Optional[str] = None
    price_steps: Optional[int] = Field(None, alias="priceSteps")
    price_unit: Optional[str] = Field(None, alias="priceUnit")
    tick_size: Optional[float] = Field(None, alias="tickSize")
    time_unit: Optional[str] = Field(None, alias="timeUnit")
    timezone: Optional[str] = None
    trade_lot: Optional[float] = Field(None, alias="tradeLot")


class NordPoolMarketApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    max_price: Optional[float] = None
    min_price: Optional[float] = None
    name: Optional[str] = None
    price_steps: Optional[int] = None
    price_unit: Optional[str] = None
    tick_size: Optional[float] = None
    time_unit: Optional[str] = None
    timezone: Optional[str] = None
    trade_lot: Optional[float] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.max_price is not None:
            properties["maxPrice"] = self.max_price
        if self.min_price is not None:
            properties["minPrice"] = self.min_price
        if self.name is not None:
            properties["name"] = self.name
        if self.price_steps is not None:
            properties["priceSteps"] = self.price_steps
        if self.price_unit is not None:
            properties["priceUnit"] = self.price_unit
        if self.tick_size is not None:
            properties["tickSize"] = self.tick_size
        if self.time_unit is not None:
            properties["timeUnit"] = self.time_unit
        if self.timezone is not None:
            properties["timezone"] = self.timezone
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
