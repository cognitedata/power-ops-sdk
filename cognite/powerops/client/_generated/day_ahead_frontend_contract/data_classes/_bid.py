from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)

if TYPE_CHECKING:
    from ._alert import Alert, AlertApply
    from ._bid import Bid, BidApply
    from ._bid_method import BidMethod, BidMethodApply
    from ._bid_table import BidTable, BidTableApply
    from ._market_price_area import MarketPriceArea, MarketPriceAreaApply


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
    """This represents the reading version of bid.

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
    method: Union[BidMethod, str, None] = Field(None, repr=False)
    price_area: Optional[str] = Field(None, alias="priceArea")
    date: Optional[datetime.date] = None
    total: Union[Bid, str, None] = Field(None, repr=False)
    start_calculation: Optional[datetime.datetime] = Field(None, alias="startCalculation")
    end_calculation: Optional[datetime.datetime] = Field(None, alias="endCalculation")
    market: Union[MarketPriceArea, str, None] = Field(None, repr=False)
    alerts: Union[list[Alert], list[str], None] = Field(default=None, repr=False)
    partials: Union[list[BidTable], list[str], None] = Field(default=None, repr=False)

    def as_apply(self) -> BidApply:
        """Convert this read version of bid to the writing version."""
        return BidApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            method=self.method.as_apply() if isinstance(self.method, DomainModel) else self.method,
            price_area=self.price_area,
            date=self.date,
            total=self.total.as_apply() if isinstance(self.total, DomainModel) else self.total,
            start_calculation=self.start_calculation,
            end_calculation=self.end_calculation,
            market=self.market.as_apply() if isinstance(self.market, DomainModel) else self.market,
            alerts=[alert.as_apply() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
            partials=[partial.as_apply() if isinstance(partial, DomainModel) else partial for partial in self.partials or []],
        )


class BidApply(DomainModelApply):
    """This represents the writing version of bid.

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
        existing_version: Fail the ingestion request if the bid version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """
    space: str = "dayAheadFrontendContractModel"
    name: Optional[str] = None
    method: Union[BidMethodApply, str, None] = Field(None, repr=False)
    price_area: Optional[str] = Field(None, alias="priceArea")
    date: Optional[datetime.date] = None
    total: Union[BidApply, str, None] = Field(None, repr=False)
    start_calculation: datetime.datetime = Field(alias="startCalculation")
    end_calculation: Optional[datetime.datetime] = Field(None, alias="endCalculation")
    market: Union[MarketPriceAreaApply, str, None] = Field(None, repr=False)
    alerts: Union[list[AlertApply], list[str], None] = Field(default=None, repr=False)
    partials: Union[list[BidTableApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "dayAheadFrontendContractModel", "Bid", "1"
        )

        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.method is not None:
            properties["method"] = {
                "space":  self.space if isinstance(self.method, str) else self.method.space,
                "externalId": self.method if isinstance(self.method, str) else self.method.external_id,
            }
        if self.price_area is not None:
            properties["priceArea"] = self.price_area
        if self.date is not None:
            properties["date"] = self.date.isoformat()
        if self.total is not None:
            properties["total"] = {
                "space":  self.space if isinstance(self.total, str) else self.total.space,
                "externalId": self.total if isinstance(self.total, str) else self.total.external_id,
            }
        if self.start_calculation is not None:
            properties["startCalculation"] = self.start_calculation.isoformat(timespec="milliseconds")
        if self.end_calculation is not None:
            properties["endCalculation"] = self.end_calculation.isoformat(timespec="milliseconds")
        if self.market is not None:
            properties["market"] = {
                "space":  self.space if isinstance(self.market, str) else self.market.space,
                "externalId": self.market if isinstance(self.market, str) else self.market.external_id,
            }

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                )],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())
        


        edge_type = dm.DirectRelationReference("dayAheadFrontendContractModel", "Bid.alerts")
        for alert in self.alerts or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, alert, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("dayAheadFrontendContractModel", "Bid.partials")
        for partial in self.partials or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, partial, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        if isinstance(self.method, DomainModelApply):
            other_resources = self.method._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        if isinstance(self.total, DomainModelApply):
            other_resources = self.total._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        if isinstance(self.market, DomainModelApply):
            other_resources = self.market._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        return resources


class BidList(DomainModelList[Bid]):
    """List of bids in the read version."""

    _INSTANCE = Bid

    def as_apply(self) -> BidApplyList:
        """Convert these read versions of bid to the writing versions."""
        return BidApplyList([node.as_apply() for node in self.data])


class BidApplyList(DomainModelApplyList[BidApply]):
    """List of bids in the writing version."""

    _INSTANCE = BidApply


def _create_bid_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    price_area: str | list[str] | None = None,
    price_area_prefix: str | None = None,
    min_date: datetime.date | None = None,
    max_date: datetime.date | None = None,
    total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    min_start_calculation: datetime.datetime | None = None,
    max_start_calculation: datetime.datetime | None = None,
    min_end_calculation: datetime.datetime | None = None,
    max_end_calculation: datetime.datetime | None = None,
    market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if method and isinstance(method, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("method"), value={"space": "dayAheadFrontendContractModel", "externalId": method}))
    if method and isinstance(method, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("method"), value={"space": method[0], "externalId": method[1]}))
    if method and isinstance(method, list) and isinstance(method[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("method"), values=[{"space": "dayAheadFrontendContractModel", "externalId": item} for item in method]))
    if method and isinstance(method, list) and isinstance(method[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("method"), values=[{"space": item[0], "externalId": item[1]} for item in method]))
    if price_area and isinstance(price_area, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("priceArea"), value=price_area))
    if price_area and isinstance(price_area, list):
        filters.append(dm.filters.In(view_id.as_property_ref("priceArea"), values=price_area))
    if price_area_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("priceArea"), value=price_area_prefix))
    if min_date or max_date:
        filters.append(dm.filters.Range(view_id.as_property_ref("date"), gte=min_date.isoformat() if min_date else None, lte=max_date.isoformat() if max_date else None))
    if total and isinstance(total, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("total"), value={"space": "dayAheadFrontendContractModel", "externalId": total}))
    if total and isinstance(total, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("total"), value={"space": total[0], "externalId": total[1]}))
    if total and isinstance(total, list) and isinstance(total[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("total"), values=[{"space": "dayAheadFrontendContractModel", "externalId": item} for item in total]))
    if total and isinstance(total, list) and isinstance(total[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("total"), values=[{"space": item[0], "externalId": item[1]} for item in total]))
    if min_start_calculation or max_start_calculation:
        filters.append(dm.filters.Range(view_id.as_property_ref("startCalculation"), gte=min_start_calculation.isoformat(timespec="milliseconds") if min_start_calculation else None, lte=max_start_calculation.isoformat(timespec="milliseconds") if max_start_calculation else None))
    if min_end_calculation or max_end_calculation:
        filters.append(dm.filters.Range(view_id.as_property_ref("endCalculation"), gte=min_end_calculation.isoformat(timespec="milliseconds") if min_end_calculation else None, lte=max_end_calculation.isoformat(timespec="milliseconds") if max_end_calculation else None))
    if market and isinstance(market, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("market"), value={"space": "dayAheadFrontendContractModel", "externalId": market}))
    if market and isinstance(market, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("market"), value={"space": market[0], "externalId": market[1]}))
    if market and isinstance(market, list) and isinstance(market[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("market"), values=[{"space": "dayAheadFrontendContractModel", "externalId": item} for item in market]))
    if market and isinstance(market, list) and isinstance(market[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("market"), values=[{"space": item[0], "externalId": item[1]} for item in market]))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
