from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, InstancesApply, TypeList

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

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "NordPoolMarket"),
            properties={
                "maxPrice": self.max_price,
                "minPrice": self.min_price,
                "priceSteps": self.price_steps,
                "priceUnit": self.price_unit,
                "tickSize": self.tick_size,
                "timeUnit": self.time_unit,
                "tradeLot": self.trade_lot,
            },
        )
        sources.append(source)

        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "Market"),
            properties={
                "name": self.name,
                "timezone": self.timezone,
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

        return InstancesApply(nodes, edges)


class NordPoolMarketList(TypeList[NordPoolMarket]):
    _NODE = NordPoolMarket
