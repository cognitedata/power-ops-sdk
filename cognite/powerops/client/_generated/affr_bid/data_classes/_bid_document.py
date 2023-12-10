from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)

if TYPE_CHECKING:
    from ._alert import Alert, AlertApply
    from ._bid_row import BidRow, BidRowApply
    from ._price_area import PriceArea, PriceAreaApply


__all__ = [
    "BidDocument",
    "BidDocumentApply",
    "BidDocumentList",
    "BidDocumentApplyList",
    "BidDocumentFields",
    "BidDocumentTextFields",
]


BidDocumentTextFields = Literal["name"]
BidDocumentFields = Literal["name", "date", "start_calculation", "end_calculation", "is_complete"]

_BIDDOCUMENT_PROPERTIES_BY_FIELD = {
    "name": "name",
    "date": "date",
    "start_calculation": "startCalculation",
    "end_calculation": "endCalculation",
    "is_complete": "isComplete",
}


class BidDocument(DomainModel):
    """This represents the reading version of bid document.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid document.
        name: Unique name for a given instance of a Bid Document. A combination of name, priceArea, date and startCalculation.
        date: The date of the Bid.
        start_calculation: Timestamp of when the Bid calculation workflow started.
        end_calculation: Timestamp of when the Bid calculation workflow completed.
        is_complete: Indicates that the Bid calculation workflow has completed (although has not necessarily succeeded).
        alerts: An array of calculation level Alerts.
        price_area: The price area field.
        bids: An array of BidRows containing the Bid data.
        created_time: The created time of the bid document node.
        last_updated_time: The last updated time of the bid document node.
        deleted_time: If present, the deleted time of the bid document node.
        version: The version of the bid document node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    name: Optional[str] = None
    date: Optional[datetime.date] = None
    start_calculation: Optional[datetime.datetime] = Field(None, alias="startCalculation")
    end_calculation: Optional[datetime.datetime] = Field(None, alias="endCalculation")
    is_complete: Optional[bool] = Field(None, alias="isComplete")
    alerts: Union[list[Alert], list[str], None] = Field(default=None, repr=False)
    price_area: Union[PriceArea, str, dm.NodeId, None] = Field(None, repr=False, alias="priceArea")
    bids: Union[list[BidRow], list[str], None] = Field(default=None, repr=False)

    def as_apply(self) -> BidDocumentApply:
        """Convert this read version of bid document to the writing version."""
        return BidDocumentApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            date=self.date,
            start_calculation=self.start_calculation,
            end_calculation=self.end_calculation,
            is_complete=self.is_complete,
            alerts=[alert.as_apply() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
            price_area=self.price_area.as_apply() if isinstance(self.price_area, DomainModel) else self.price_area,
            bids=[bid.as_apply() if isinstance(bid, DomainModel) else bid for bid in self.bids or []],
        )


class BidDocumentApply(DomainModelApply):
    """This represents the writing version of bid document.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid document.
        name: Unique name for a given instance of a Bid Document. A combination of name, priceArea, date and startCalculation.
        date: The date of the Bid.
        start_calculation: Timestamp of when the Bid calculation workflow started.
        end_calculation: Timestamp of when the Bid calculation workflow completed.
        is_complete: Indicates that the Bid calculation workflow has completed (although has not necessarily succeeded).
        alerts: An array of calculation level Alerts.
        price_area: The price area field.
        bids: An array of BidRows containing the Bid data.
        existing_version: Fail the ingestion request if the bid document version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    name: Optional[str] = None
    date: Optional[datetime.date] = None
    start_calculation: Optional[datetime.datetime] = Field(None, alias="startCalculation")
    end_calculation: Optional[datetime.datetime] = Field(None, alias="endCalculation")
    is_complete: Optional[bool] = Field(None, alias="isComplete")
    alerts: Union[list[AlertApply], list[str], None] = Field(default=None, repr=False)
    price_area: Union[PriceAreaApply, str, dm.NodeId, None] = Field(None, repr=False, alias="priceArea")
    bids: Union[list[BidRowApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "power-ops-afrr-bid", "BidDocument", "1"
        )

        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.date is not None:
            properties["date"] = self.date.isoformat()
        if self.start_calculation is not None:
            properties["startCalculation"] = self.start_calculation.isoformat(timespec="milliseconds")
        if self.end_calculation is not None:
            properties["endCalculation"] = self.end_calculation.isoformat(timespec="milliseconds")
        if self.is_complete is not None:
            properties["isComplete"] = self.is_complete
        if self.price_area is not None:
            properties["priceArea"] = {
                "space": self.space if isinstance(self.price_area, str) else self.price_area.space,
                "externalId": self.price_area if isinstance(self.price_area, str) else self.price_area.external_id,
            }

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=dm.DirectRelationReference("power-ops-types", "AFRRBidDocument"),
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        edge_type = dm.DirectRelationReference("power-ops-types", "calculationIssue")
        for alert in self.alerts or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, alert, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("power-ops-types", "PartialBid")
        for bid in self.bids or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, bid, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        if isinstance(self.price_area, DomainModelApply):
            other_resources = self.price_area._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        return resources


class BidDocumentList(DomainModelList[BidDocument]):
    """List of bid documents in the read version."""

    _INSTANCE = BidDocument

    def as_apply(self) -> BidDocumentApplyList:
        """Convert these read versions of bid document to the writing versions."""
        return BidDocumentApplyList([node.as_apply() for node in self.data])


class BidDocumentApplyList(DomainModelApplyList[BidDocumentApply]):
    """List of bid documents in the writing version."""

    _INSTANCE = BidDocumentApply


def _create_bid_document_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    min_date: datetime.date | None = None,
    max_date: datetime.date | None = None,
    min_start_calculation: datetime.datetime | None = None,
    max_start_calculation: datetime.datetime | None = None,
    min_end_calculation: datetime.datetime | None = None,
    max_end_calculation: datetime.datetime | None = None,
    is_complete: bool | None = None,
    price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if min_date or max_date:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("date"),
                gte=min_date.isoformat() if min_date else None,
                lte=max_date.isoformat() if max_date else None,
            )
        )
    if min_start_calculation or max_start_calculation:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("startCalculation"),
                gte=min_start_calculation.isoformat(timespec="milliseconds") if min_start_calculation else None,
                lte=max_start_calculation.isoformat(timespec="milliseconds") if max_start_calculation else None,
            )
        )
    if min_end_calculation or max_end_calculation:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("endCalculation"),
                gte=min_end_calculation.isoformat(timespec="milliseconds") if min_end_calculation else None,
                lte=max_end_calculation.isoformat(timespec="milliseconds") if max_end_calculation else None,
            )
        )
    if is_complete and isinstance(is_complete, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isComplete"), value=is_complete))
    if price_area and isinstance(price_area, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("priceArea"), value={"space": "power-ops-afrr-bid", "externalId": price_area}
            )
        )
    if price_area and isinstance(price_area, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("priceArea"), value={"space": price_area[0], "externalId": price_area[1]}
            )
        )
    if price_area and isinstance(price_area, list) and isinstance(price_area[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("priceArea"),
                values=[{"space": "power-ops-afrr-bid", "externalId": item} for item in price_area],
            )
        )
    if price_area and isinstance(price_area, list) and isinstance(price_area[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("priceArea"),
                values=[{"space": item[0], "externalId": item[1]} for item in price_area],
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
