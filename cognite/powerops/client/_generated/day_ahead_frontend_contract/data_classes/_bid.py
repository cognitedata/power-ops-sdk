from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._alert import AlertApply
    from ._bid import BidApply
    from ._bid_table import BidTableApply
    from ._market_price_area import MarketPriceAreaApply

__all__ = ["Bid", "BidApply", "BidList", "BidApplyList", "BidFields", "BidTextFields"]


BidTextFields = Literal["name", "price_area"]
BidFields = Literal["name", "price_area", "date", "start_calculation", "end_calculation"]

_BID_PROPERTIES_BY_FIELD = {
    "name": "name",
    "price_area": "priceArea",
    "date": "date",
    "start_calculation": "startCalculation",
    "end_calculation": "endCalculation",
}


class Bid(DomainModel):
    """This represent a read version of bid.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid.
        name: The name field.
        method: The method field.
        price_area: The price area field.
        date: The date field.
        total: The total field.
        start_calculation: The start calculation field.
        end_calculation: The end calculation field.
        market: The market field.
        alerts: The alert field.
        partials: The partial field.
        created_time: The created time of the bid node.
        last_updated_time: The last updated time of the bid node.
        deleted_time: If present, the deleted time of the bid node.
        version: The version of the bid node.
    """

    space: str = "dayAheadFrontendContractModel"
    name: Optional[str] = None
    method: Optional[str] = None
    price_area: Optional[str] = Field(None, alias="priceArea")
    date: Optional[datetime.date] = None
    total: Optional[str] = None
    start_calculation: Optional[datetime.datetime] = Field(None, alias="startCalculation")
    end_calculation: Optional[datetime.datetime] = Field(None, alias="endCalculation")
    market: Optional[str] = None
    alerts: Optional[list[str]] = None
    partials: Optional[list[str]] = None

    def as_apply(self) -> BidApply:
        """Convert this read version of bid to a write version."""
        return BidApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            method=self.method,
            price_area=self.price_area,
            date=self.date,
            total=self.total,
            start_calculation=self.start_calculation,
            end_calculation=self.end_calculation,
            market=self.market,
            alerts=self.alerts,
            partials=self.partials,
        )


class BidApply(DomainModelApply):
    """This represent a write version of bid.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid.
        name: The name field.
        method: The method field.
        price_area: The price area field.
        date: The date field.
        total: The total field.
        start_calculation: The start calculation field.
        end_calculation: The end calculation field.
        market: The market field.
        alerts: The alert field.
        partials: The partial field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "dayAheadFrontendContractModel"
    name: Optional[str] = None
    method: Union[BidApply, str, None] = Field(None, repr=False)
    price_area: Optional[str] = Field(None, alias="priceArea")
    date: Optional[datetime.date] = None
    total: Union[BidApply, str, None] = Field(None, repr=False)
    start_calculation: datetime.datetime = Field(alias="startCalculation")
    end_calculation: Optional[datetime.datetime] = Field(None, alias="endCalculation")
    market: Union[MarketPriceAreaApply, str, None] = Field(None, repr=False)
    alerts: Union[list[AlertApply], list[str], None] = Field(default=None, repr=False)
    partials: Union[list[BidTableApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.method is not None:
            properties["method"] = {
                "space": self.space if isinstance(self.method, str) else self.method.space,
                "externalId": self.method if isinstance(self.method, str) else self.method.external_id,
            }
        if self.price_area is not None:
            properties["priceArea"] = self.price_area
        if self.date is not None:
            properties["date"] = self.date.isoformat()
        if self.total is not None:
            properties["total"] = {
                "space": self.space if isinstance(self.total, str) else self.total.space,
                "externalId": self.total if isinstance(self.total, str) else self.total.external_id,
            }
        if self.start_calculation is not None:
            properties["startCalculation"] = self.start_calculation.isoformat()
        if self.end_calculation is not None:
            properties["endCalculation"] = self.end_calculation.isoformat()
        if self.market is not None:
            properties["market"] = {
                "space": self.space if isinstance(self.market, str) else self.market.space,
                "externalId": self.market if isinstance(self.market, str) else self.market.external_id,
            }
        if properties:
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("dayAheadFrontendContractModel", "Bid", "1"),
                properties=properties,
            )
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[source],
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        for alert in self.alerts or []:
            edge = self._create_alert_edge(alert)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(alert, DomainModelApply):
                instances = alert._to_instances_apply(cache, view_by_write_class)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for partial in self.partials or []:
            edge = self._create_partial_edge(partial)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(partial, DomainModelApply):
                instances = partial._to_instances_apply(cache, view_by_write_class)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.method, DomainModelApply):
            instances = self.method._to_instances_apply(cache, view_by_write_class)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.total, DomainModelApply):
            instances = self.total._to_instances_apply(cache, view_by_write_class)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.market, DomainModelApply):
            instances = self.market._to_instances_apply(cache, view_by_write_class)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_alert_edge(self, alert: Union[str, AlertApply]) -> dm.EdgeApply:
        if isinstance(alert, str):
            end_space, end_node_ext_id = self.space, alert
        elif isinstance(alert, DomainModelApply):
            end_space, end_node_ext_id = alert.space, alert.external_id
        else:
            raise TypeError(f"Expected str or AlertApply, got {type(alert)}")

        return dm.EdgeApply(
            space=self.space,
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("dayAheadFrontendContractModel", "Bid.alerts"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference(end_space, end_node_ext_id),
        )

    def _create_partial_edge(self, partial: Union[str, BidTableApply]) -> dm.EdgeApply:
        if isinstance(partial, str):
            end_space, end_node_ext_id = self.space, partial
        elif isinstance(partial, DomainModelApply):
            end_space, end_node_ext_id = partial.space, partial.external_id
        else:
            raise TypeError(f"Expected str or BidTableApply, got {type(partial)}")

        return dm.EdgeApply(
            space=self.space,
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("dayAheadFrontendContractModel", "Bid.partials"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference(end_space, end_node_ext_id),
        )


class BidList(TypeList[Bid]):
    """List of bids in read version."""

    _NODE = Bid

    def as_apply(self) -> BidApplyList:
        """Convert this read version of bid to a write version."""
        return BidApplyList([node.as_apply() for node in self.data])


class BidApplyList(TypeApplyList[BidApply]):
    """List of bids in write version."""

    _NODE = BidApply
