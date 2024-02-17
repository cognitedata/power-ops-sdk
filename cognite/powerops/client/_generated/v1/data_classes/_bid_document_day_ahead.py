from __future__ import annotations

import datetime
import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    ResourcesWrite,
)

if TYPE_CHECKING:
    from ._alert import Alert, AlertWrite
    from ._bid_matrix import BidMatrix, BidMatrixWrite
    from ._bid_method_day_ahead import BidMethodDayAhead, BidMethodDayAheadWrite
    from ._price_area import PriceArea, PriceAreaWrite


__all__ = [
    "BidDocumentDayAhead",
    "BidDocumentDayAheadWrite",
    "BidDocumentDayAheadApply",
    "BidDocumentDayAheadList",
    "BidDocumentDayAheadWriteList",
    "BidDocumentDayAheadApplyList",
    "BidDocumentDayAheadFields",
    "BidDocumentDayAheadTextFields",
]


BidDocumentDayAheadTextFields = Literal["name"]
BidDocumentDayAheadFields = Literal["name", "delivery_date", "start_calculation", "end_calculation", "is_complete"]

_BIDDOCUMENTDAYAHEAD_PROPERTIES_BY_FIELD = {
    "name": "name",
    "delivery_date": "deliveryDate",
    "start_calculation": "startCalculation",
    "end_calculation": "endCalculation",
    "is_complete": "isComplete",
}


class BidDocumentDayAhead(DomainModel):
    """This represents the reading version of bid document day ahead.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid document day ahead.
        data_record: The data record of the bid document day ahead node.
        name: Unique name for a given instance of a Bid Document. A combination of name, priceArea, date and startCalculation.
        delivery_date: The date of the Bid.
        start_calculation: Timestamp of when the Bid calculation workflow started.
        end_calculation: Timestamp of when the Bid calculation workflow completed.
        is_complete: Indicates that the Bid calculation workflow has completed (although has not necessarily succeeded).
        alerts: An array of calculation level Alerts.
        price_area: The price area field.
        method: The method field.
        total: The total field.
        partials: The partial field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "DayAheadBidDocument"
    )
    name: Optional[str] = None
    delivery_date: datetime.date = Field(alias="deliveryDate")
    start_calculation: Optional[datetime.datetime] = Field(None, alias="startCalculation")
    end_calculation: Optional[datetime.datetime] = Field(None, alias="endCalculation")
    is_complete: Optional[bool] = Field(None, alias="isComplete")
    alerts: Union[list[Alert], list[str], None] = Field(default=None, repr=False)
    price_area: Union[PriceArea, str, dm.NodeId, None] = Field(None, repr=False, alias="priceArea")
    method: Union[BidMethodDayAhead, str, dm.NodeId, None] = Field(None, repr=False)
    total: Union[BidMatrix, str, dm.NodeId, None] = Field(None, repr=False)
    partials: Union[list[BidMatrix], list[str], None] = Field(default=None, repr=False)

    def as_write(self) -> BidDocumentDayAheadWrite:
        """Convert this read version of bid document day ahead to the writing version."""
        return BidDocumentDayAheadWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            delivery_date=self.delivery_date,
            start_calculation=self.start_calculation,
            end_calculation=self.end_calculation,
            is_complete=self.is_complete,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
            price_area=self.price_area.as_write() if isinstance(self.price_area, DomainModel) else self.price_area,
            method=self.method.as_write() if isinstance(self.method, DomainModel) else self.method,
            total=self.total.as_write() if isinstance(self.total, DomainModel) else self.total,
            partials=[
                partial.as_write() if isinstance(partial, DomainModel) else partial for partial in self.partials or []
            ],
        )

    def as_apply(self) -> BidDocumentDayAheadWrite:
        """Convert this read version of bid document day ahead to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidDocumentDayAheadWrite(DomainModelWrite):
    """This represents the writing version of bid document day ahead.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid document day ahead.
        data_record: The data record of the bid document day ahead node.
        name: Unique name for a given instance of a Bid Document. A combination of name, priceArea, date and startCalculation.
        delivery_date: The date of the Bid.
        start_calculation: Timestamp of when the Bid calculation workflow started.
        end_calculation: Timestamp of when the Bid calculation workflow completed.
        is_complete: Indicates that the Bid calculation workflow has completed (although has not necessarily succeeded).
        alerts: An array of calculation level Alerts.
        price_area: The price area field.
        method: The method field.
        total: The total field.
        partials: The partial field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "DayAheadBidDocument"
    )
    name: Optional[str] = None
    delivery_date: datetime.date = Field(alias="deliveryDate")
    start_calculation: Optional[datetime.datetime] = Field(None, alias="startCalculation")
    end_calculation: Optional[datetime.datetime] = Field(None, alias="endCalculation")
    is_complete: Optional[bool] = Field(None, alias="isComplete")
    alerts: Union[list[AlertWrite], list[str], None] = Field(default=None, repr=False)
    price_area: Union[PriceAreaWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="priceArea")
    method: Union[BidMethodDayAheadWrite, str, dm.NodeId, None] = Field(None, repr=False)
    total: Union[BidMatrixWrite, str, dm.NodeId, None] = Field(None, repr=False)
    partials: Union[list[BidMatrixWrite], list[str], None] = Field(default=None, repr=False)

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            BidDocumentDayAhead, dm.ViewId("sp_powerops_models", "BidDocumentDayAhead", "1")
        )

        properties: dict[str, Any] = {}

        if self.name is not None or write_none:
            properties["name"] = self.name

        if self.delivery_date is not None:
            properties["deliveryDate"] = self.delivery_date.isoformat() if self.delivery_date else None

        if self.start_calculation is not None or write_none:
            properties["startCalculation"] = (
                self.start_calculation.isoformat(timespec="milliseconds") if self.start_calculation else None
            )

        if self.end_calculation is not None or write_none:
            properties["endCalculation"] = (
                self.end_calculation.isoformat(timespec="milliseconds") if self.end_calculation else None
            )

        if self.is_complete is not None or write_none:
            properties["isComplete"] = self.is_complete

        if self.price_area is not None:
            properties["priceArea"] = {
                "space": self.space if isinstance(self.price_area, str) else self.price_area.space,
                "externalId": self.price_area if isinstance(self.price_area, str) else self.price_area.external_id,
            }

        if self.method is not None:
            properties["method"] = {
                "space": self.space if isinstance(self.method, str) else self.method.space,
                "externalId": self.method if isinstance(self.method, str) else self.method.external_id,
            }

        if self.total is not None:
            properties["total"] = {
                "space": self.space if isinstance(self.total, str) else self.total.space,
                "externalId": self.total if isinstance(self.total, str) else self.total.external_id,
            }

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        edge_type = dm.DirectRelationReference("sp_powerops_types", "calculationIssue")
        for alert in self.alerts or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache, start_node=self, end_node=alert, edge_type=edge_type, view_by_read_class=view_by_read_class
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("sp_powerops_types", "partialBid")
        for partial in self.partials or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache, start_node=self, end_node=partial, edge_type=edge_type, view_by_read_class=view_by_read_class
            )
            resources.extend(other_resources)

        if isinstance(self.price_area, DomainModelWrite):
            other_resources = self.price_area._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.method, DomainModelWrite):
            other_resources = self.method._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.total, DomainModelWrite):
            other_resources = self.total._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class BidDocumentDayAheadApply(BidDocumentDayAheadWrite):
    def __new__(cls, *args, **kwargs) -> BidDocumentDayAheadApply:
        warnings.warn(
            "BidDocumentDayAheadApply is deprecated and will be removed in v1.0. Use BidDocumentDayAheadWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "BidDocumentDayAhead.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class BidDocumentDayAheadList(DomainModelList[BidDocumentDayAhead]):
    """List of bid document day aheads in the read version."""

    _INSTANCE = BidDocumentDayAhead

    def as_write(self) -> BidDocumentDayAheadWriteList:
        """Convert these read versions of bid document day ahead to the writing versions."""
        return BidDocumentDayAheadWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> BidDocumentDayAheadWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidDocumentDayAheadWriteList(DomainModelWriteList[BidDocumentDayAheadWrite]):
    """List of bid document day aheads in the writing version."""

    _INSTANCE = BidDocumentDayAheadWrite


class BidDocumentDayAheadApplyList(BidDocumentDayAheadWriteList): ...


def _create_bid_document_day_ahead_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    min_delivery_date: datetime.date | None = None,
    max_delivery_date: datetime.date | None = None,
    min_start_calculation: datetime.datetime | None = None,
    max_start_calculation: datetime.datetime | None = None,
    min_end_calculation: datetime.datetime | None = None,
    max_end_calculation: datetime.datetime | None = None,
    is_complete: bool | None = None,
    price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if min_delivery_date is not None or max_delivery_date is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("deliveryDate"),
                gte=min_delivery_date.isoformat() if min_delivery_date else None,
                lte=max_delivery_date.isoformat() if max_delivery_date else None,
            )
        )
    if min_start_calculation is not None or max_start_calculation is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("startCalculation"),
                gte=min_start_calculation.isoformat(timespec="milliseconds") if min_start_calculation else None,
                lte=max_start_calculation.isoformat(timespec="milliseconds") if max_start_calculation else None,
            )
        )
    if min_end_calculation is not None or max_end_calculation is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("endCalculation"),
                gte=min_end_calculation.isoformat(timespec="milliseconds") if min_end_calculation else None,
                lte=max_end_calculation.isoformat(timespec="milliseconds") if max_end_calculation else None,
            )
        )
    if isinstance(is_complete, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isComplete"), value=is_complete))
    if price_area and isinstance(price_area, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("priceArea"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": price_area}
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
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in price_area],
            )
        )
    if price_area and isinstance(price_area, list) and isinstance(price_area[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("priceArea"),
                values=[{"space": item[0], "externalId": item[1]} for item in price_area],
            )
        )
    if method and isinstance(method, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("method"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": method}
            )
        )
    if method and isinstance(method, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("method"), value={"space": method[0], "externalId": method[1]})
        )
    if method and isinstance(method, list) and isinstance(method[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("method"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in method],
            )
        )
    if method and isinstance(method, list) and isinstance(method[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("method"), values=[{"space": item[0], "externalId": item[1]} for item in method]
            )
        )
    if total and isinstance(total, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("total"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": total}
            )
        )
    if total and isinstance(total, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("total"), value={"space": total[0], "externalId": total[1]})
        )
    if total and isinstance(total, list) and isinstance(total[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("total"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in total],
            )
        )
    if total and isinstance(total, list) and isinstance(total[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("total"), values=[{"space": item[0], "externalId": item[1]} for item in total]
            )
        )
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
