from __future__ import annotations

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
    from ._bid_method import BidMethod, BidMethodWrite
    from ._bid_row import BidRow, BidRowWrite


__all__ = [
    "BidRow",
    "BidRowWrite",
    "BidRowApply",
    "BidRowList",
    "BidRowWriteList",
    "BidRowApplyList",
    "BidRowFields",
    "BidRowTextFields",
]


BidRowTextFields = Literal["product", "exclusive_group_id", "asset_type", "asset_id"]
BidRowFields = Literal[
    "price",
    "quantity_per_hour",
    "product",
    "is_divisible",
    "min_quantity",
    "is_block",
    "exclusive_group_id",
    "asset_type",
    "asset_id",
]

_BIDROW_PROPERTIES_BY_FIELD = {
    "price": "price",
    "quantity_per_hour": "quantityPerHour",
    "product": "product",
    "is_divisible": "isDivisible",
    "min_quantity": "minQuantity",
    "is_block": "isBlock",
    "exclusive_group_id": "exclusiveGroupId",
    "asset_type": "assetType",
    "asset_id": "assetId",
}


class BidRow(DomainModel):
    """This represents the reading version of bid row.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid row.
        data_record: The data record of the bid row node.
        price: Price in EUR/MW/h, rounded to nearest price step (0.1?)
        quantity_per_hour: The capacity offered, per hour, in MW, rounded to nearest step size (5?)
        product: The product field.
        is_divisible: The is divisible field.
        min_quantity: Min quantity, per hour. Only relevant for divisible Bids. The minimum capacity that must be accepted; this must be lower than capacityPerHour and is rounded to the nearest step (5 MW?)).
        is_block: Indication if the row is part of a Block bid. If true: quantityPerHour must have the same value for consecutive hours (and no breaks). Block bids must be accepted for all hours or none.
        exclusive_group_id: Other bids with the same ID are part of an exclusive group - only one of them can be accepted, and they must have the same direction (product). Not allowed for block bids.
        linked_bid: The linked bid must have the opposite direction (link means that both or none must be accepted). Should be bi-directional.
        asset_type: The asset type field.
        asset_id: The asset id field.
        method: The method field.
        alerts: An array of associated alerts.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power-ops-types", "AFRRBidRow")
    price: Optional[float] = None
    quantity_per_hour: Optional[list[float]] = Field(None, alias="quantityPerHour")
    product: Optional[str] = None
    is_divisible: Optional[bool] = Field(None, alias="isDivisible")
    min_quantity: Optional[list[float]] = Field(None, alias="minQuantity")
    is_block: Optional[bool] = Field(None, alias="isBlock")
    exclusive_group_id: Optional[str] = Field(None, alias="exclusiveGroupId")
    linked_bid: Union[BidRow, str, dm.NodeId, None] = Field(None, repr=False, alias="linkedBid")
    asset_type: Optional[str] = Field(None, alias="assetType")
    asset_id: Optional[str] = Field(None, alias="assetId")
    method: Union[BidMethod, str, dm.NodeId, None] = Field(None, repr=False)
    alerts: Union[list[Alert], list[str], None] = Field(default=None, repr=False)

    def as_write(self) -> BidRowWrite:
        """Convert this read version of bid row to the writing version."""
        return BidRowWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            price=self.price,
            quantity_per_hour=self.quantity_per_hour,
            product=self.product,
            is_divisible=self.is_divisible,
            min_quantity=self.min_quantity,
            is_block=self.is_block,
            exclusive_group_id=self.exclusive_group_id,
            linked_bid=self.linked_bid.as_write() if isinstance(self.linked_bid, DomainModel) else self.linked_bid,
            asset_type=self.asset_type,
            asset_id=self.asset_id,
            method=self.method.as_write() if isinstance(self.method, DomainModel) else self.method,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
        )

    def as_apply(self) -> BidRowWrite:
        """Convert this read version of bid row to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidRowWrite(DomainModelWrite):
    """This represents the writing version of bid row.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid row.
        data_record: The data record of the bid row node.
        price: Price in EUR/MW/h, rounded to nearest price step (0.1?)
        quantity_per_hour: The capacity offered, per hour, in MW, rounded to nearest step size (5?)
        product: The product field.
        is_divisible: The is divisible field.
        min_quantity: Min quantity, per hour. Only relevant for divisible Bids. The minimum capacity that must be accepted; this must be lower than capacityPerHour and is rounded to the nearest step (5 MW?)).
        is_block: Indication if the row is part of a Block bid. If true: quantityPerHour must have the same value for consecutive hours (and no breaks). Block bids must be accepted for all hours or none.
        exclusive_group_id: Other bids with the same ID are part of an exclusive group - only one of them can be accepted, and they must have the same direction (product). Not allowed for block bids.
        linked_bid: The linked bid must have the opposite direction (link means that both or none must be accepted). Should be bi-directional.
        asset_type: The asset type field.
        asset_id: The asset id field.
        method: The method field.
        alerts: An array of associated alerts.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power-ops-types", "AFRRBidRow")
    price: Optional[float] = None
    quantity_per_hour: Optional[list[float]] = Field(None, alias="quantityPerHour")
    product: Optional[str] = None
    is_divisible: Optional[bool] = Field(None, alias="isDivisible")
    min_quantity: Optional[list[float]] = Field(None, alias="minQuantity")
    is_block: Optional[bool] = Field(None, alias="isBlock")
    exclusive_group_id: Optional[str] = Field(None, alias="exclusiveGroupId")
    linked_bid: Union[BidRowWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="linkedBid")
    asset_type: Optional[str] = Field(None, alias="assetType")
    asset_id: Optional[str] = Field(None, alias="assetId")
    method: Union[BidMethodWrite, str, dm.NodeId, None] = Field(None, repr=False)
    alerts: Union[list[AlertWrite], list[str], None] = Field(default=None, repr=False)

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(BidRow, dm.ViewId("power-ops-afrr-bid", "BidRow", "1"))

        properties: dict[str, Any] = {}

        if self.price is not None or write_none:
            properties["price"] = self.price

        if self.quantity_per_hour is not None or write_none:
            properties["quantityPerHour"] = self.quantity_per_hour

        if self.product is not None or write_none:
            properties["product"] = self.product

        if self.is_divisible is not None or write_none:
            properties["isDivisible"] = self.is_divisible

        if self.min_quantity is not None or write_none:
            properties["minQuantity"] = self.min_quantity

        if self.is_block is not None or write_none:
            properties["isBlock"] = self.is_block

        if self.exclusive_group_id is not None or write_none:
            properties["exclusiveGroupId"] = self.exclusive_group_id

        if self.linked_bid is not None:
            properties["linkedBid"] = {
                "space": self.space if isinstance(self.linked_bid, str) else self.linked_bid.space,
                "externalId": self.linked_bid if isinstance(self.linked_bid, str) else self.linked_bid.external_id,
            }

        if self.asset_type is not None or write_none:
            properties["assetType"] = self.asset_type

        if self.asset_id is not None or write_none:
            properties["assetId"] = self.asset_id

        if self.method is not None:
            properties["method"] = {
                "space": self.space if isinstance(self.method, str) else self.method.space,
                "externalId": self.method if isinstance(self.method, str) else self.method.external_id,
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

        edge_type = dm.DirectRelationReference("power-ops-types", "calculationIssue")
        for alert in self.alerts or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache, start_node=self, end_node=alert, edge_type=edge_type, view_by_read_class=view_by_read_class
            )
            resources.extend(other_resources)

        if isinstance(self.linked_bid, DomainModelWrite):
            other_resources = self.linked_bid._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.method, DomainModelWrite):
            other_resources = self.method._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class BidRowApply(BidRowWrite):
    def __new__(cls, *args, **kwargs) -> BidRowApply:
        warnings.warn(
            "BidRowApply is deprecated and will be removed in v1.0. Use BidRowWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "BidRow.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class BidRowList(DomainModelList[BidRow]):
    """List of bid rows in the read version."""

    _INSTANCE = BidRow

    def as_write(self) -> BidRowWriteList:
        """Convert these read versions of bid row to the writing versions."""
        return BidRowWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> BidRowWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidRowWriteList(DomainModelWriteList[BidRowWrite]):
    """List of bid rows in the writing version."""

    _INSTANCE = BidRowWrite


class BidRowApplyList(BidRowWriteList): ...


def _create_bid_row_filter(
    view_id: dm.ViewId,
    min_price: float | None = None,
    max_price: float | None = None,
    product: str | list[str] | None = None,
    product_prefix: str | None = None,
    is_divisible: bool | None = None,
    is_block: bool | None = None,
    exclusive_group_id: str | list[str] | None = None,
    exclusive_group_id_prefix: str | None = None,
    linked_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    asset_type: str | list[str] | None = None,
    asset_type_prefix: str | None = None,
    asset_id: str | list[str] | None = None,
    asset_id_prefix: str | None = None,
    method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_price is not None or max_price is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("price"), gte=min_price, lte=max_price))
    if isinstance(product, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("product"), value=product))
    if product and isinstance(product, list):
        filters.append(dm.filters.In(view_id.as_property_ref("product"), values=product))
    if product_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("product"), value=product_prefix))
    if isinstance(is_divisible, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isDivisible"), value=is_divisible))
    if isinstance(is_block, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isBlock"), value=is_block))
    if isinstance(exclusive_group_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("exclusiveGroupId"), value=exclusive_group_id))
    if exclusive_group_id and isinstance(exclusive_group_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("exclusiveGroupId"), values=exclusive_group_id))
    if exclusive_group_id_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("exclusiveGroupId"), value=exclusive_group_id_prefix))
    if linked_bid and isinstance(linked_bid, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("linkedBid"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": linked_bid}
            )
        )
    if linked_bid and isinstance(linked_bid, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("linkedBid"), value={"space": linked_bid[0], "externalId": linked_bid[1]}
            )
        )
    if linked_bid and isinstance(linked_bid, list) and isinstance(linked_bid[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("linkedBid"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in linked_bid],
            )
        )
    if linked_bid and isinstance(linked_bid, list) and isinstance(linked_bid[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("linkedBid"),
                values=[{"space": item[0], "externalId": item[1]} for item in linked_bid],
            )
        )
    if isinstance(asset_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("assetType"), value=asset_type))
    if asset_type and isinstance(asset_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("assetType"), values=asset_type))
    if asset_type_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("assetType"), value=asset_type_prefix))
    if isinstance(asset_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("assetId"), value=asset_id))
    if asset_id and isinstance(asset_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("assetId"), values=asset_id))
    if asset_id_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("assetId"), value=asset_id_prefix))
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
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
