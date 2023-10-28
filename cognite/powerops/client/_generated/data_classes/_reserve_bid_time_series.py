from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = [
    "ReserveBidTimeSeries",
    "ReserveBidTimeSeriesApply",
    "ReserveBidTimeSeriesList",
    "ReserveBidTimeSeriesApplyList",
    "ReserveBidTimeSeriesFields",
    "ReserveBidTimeSeriesTextFields",
]


ReserveBidTimeSeriesTextFields = Literal["bid_type", "measure_unit", "currency", "direction_name", "reserve_object"]
ReserveBidTimeSeriesFields = Literal[
    "bid_type", "measure_unit", "currency", "price", "direction_name", "reserve_object"
]

_RESERVEBIDTIMESERIES_PROPERTIES_BY_FIELD = {
    "bid_type": "BidType",
    "measure_unit": "MeasureUnit",
    "currency": "Currency",
    "price": "Price",
    "direction_name": "DirectionName",
    "reserve_object": "ReserveObject",
}


class ReserveBidTimeSeries(DomainModel):
    space: str = "power-ops"
    bid_type: Optional[list[str]] = Field(None, alias="BidType")
    measure_unit: Optional[list[str]] = Field(None, alias="MeasureUnit")
    currency: Optional[list[str]] = Field(None, alias="Currency")
    price: Optional[list[float]] = Field(None, alias="Price")
    direction_name: Optional[list[str]] = Field(None, alias="DirectionName")
    reserve_object: Optional[list[str]] = Field(None, alias="ReserveObject")

    def as_apply(self) -> ReserveBidTimeSeriesApply:
        return ReserveBidTimeSeriesApply(
            external_id=self.external_id,
            bid_type=self.bid_type,
            measure_unit=self.measure_unit,
            currency=self.currency,
            price=self.price,
            direction_name=self.direction_name,
            reserve_object=self.reserve_object,
        )


class ReserveBidTimeSeriesApply(DomainModelApply):
    space: str = "power-ops"
    bid_type: Optional[list[str]] = Field(None, alias="BidType")
    measure_unit: Optional[list[str]] = Field(None, alias="MeasureUnit")
    currency: Optional[list[str]] = Field(None, alias="Currency")
    price: Optional[list[float]] = Field(None, alias="Price")
    direction_name: Optional[list[str]] = Field(None, alias="DirectionName")
    reserve_object: Optional[list[str]] = Field(None, alias="ReserveObject")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.bid_type is not None:
            properties["BidType"] = self.bid_type
        if self.measure_unit is not None:
            properties["MeasureUnit"] = self.measure_unit
        if self.currency is not None:
            properties["Currency"] = self.currency
        if self.price is not None:
            properties["Price"] = self.price
        if self.direction_name is not None:
            properties["DirectionName"] = self.direction_name
        if self.reserve_object is not None:
            properties["ReserveObject"] = self.reserve_object
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "ReserveBidTimeSeries"),
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


class ReserveBidTimeSeriesList(TypeList[ReserveBidTimeSeries]):
    _NODE = ReserveBidTimeSeries

    def as_apply(self) -> ReserveBidTimeSeriesApplyList:
        return ReserveBidTimeSeriesApplyList([node.as_apply() for node in self.data])


class ReserveBidTimeSeriesApplyList(TypeApplyList[ReserveBidTimeSeriesApply]):
    _NODE = ReserveBidTimeSeriesApply
