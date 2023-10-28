from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._periods import PeriodsApply

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
    bid_type: Optional[str] = Field(None, alias="BidType")
    measure_unit: Optional[str] = Field(None, alias="MeasureUnit")
    currency: Optional[str] = Field(None, alias="Currency")
    price: Optional[float] = Field(None, alias="Price")
    direction_name: Optional[str] = Field(None, alias="DirectionName")
    reserve_object: Optional[str] = Field(None, alias="ReserveObject")
    periods: Optional[list[str]] = Field(None, alias="Periods")

    def as_apply(self) -> ReserveBidTimeSeriesApply:
        return ReserveBidTimeSeriesApply(
            external_id=self.external_id,
            bid_type=self.bid_type,
            measure_unit=self.measure_unit,
            currency=self.currency,
            price=self.price,
            direction_name=self.direction_name,
            reserve_object=self.reserve_object,
            periods=self.periods,
        )


class ReserveBidTimeSeriesApply(DomainModelApply):
    space: str = "power-ops"
    bid_type: Optional[str] = Field(None, alias="BidType")
    measure_unit: Optional[str] = Field(None, alias="MeasureUnit")
    currency: Optional[str] = Field(None, alias="Currency")
    price: Optional[float] = Field(None, alias="Price")
    direction_name: Optional[str] = Field(None, alias="DirectionName")
    reserve_object: Optional[str] = Field(None, alias="ReserveObject")
    periods: Union[list[PeriodsApply], list[str], None] = Field(default=None, repr=False, alias="Periods")

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

        for period in self.periods or []:
            edge = self._create_period_edge(period)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(period, DomainModelApply):
                instances = period._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_period_edge(self, period: Union[str, PeriodsApply]) -> dm.EdgeApply:
        if isinstance(period, str):
            end_node_ext_id = period
        elif isinstance(period, DomainModelApply):
            end_node_ext_id = period.external_id
        else:
            raise TypeError(f"Expected str or PeriodsApply, got {type(period)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "ReserveBidTimeSeries.Periods"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class ReserveBidTimeSeriesList(TypeList[ReserveBidTimeSeries]):
    _NODE = ReserveBidTimeSeries

    def as_apply(self) -> ReserveBidTimeSeriesApplyList:
        return ReserveBidTimeSeriesApplyList([node.as_apply() for node in self.data])


class ReserveBidTimeSeriesApplyList(TypeApplyList[ReserveBidTimeSeriesApply]):
    _NODE = ReserveBidTimeSeriesApply
