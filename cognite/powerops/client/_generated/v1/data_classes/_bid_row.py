from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Literal,  no_type_check, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
)

if TYPE_CHECKING:
    from ._alert import Alert, AlertGraphQL, AlertWrite
    from ._power_asset import PowerAsset, PowerAssetGraphQL, PowerAssetWrite


__all__ = [
    "BidRow",
    "BidRowWrite",
    "BidRowApply",
    "BidRowList",
    "BidRowWriteList",
    "BidRowApplyList",
    "BidRowFields",
    "BidRowTextFields",
    "BidRowGraphQL",
]


BidRowTextFields = Literal["product", "exclusive_group_id"]
BidRowFields = Literal["price", "quantity_per_hour", "product", "is_divisible", "min_quantity", "is_block", "exclusive_group_id"]

_BIDROW_PROPERTIES_BY_FIELD = {
    "price": "price",
    "quantity_per_hour": "quantityPerHour",
    "product": "product",
    "is_divisible": "isDivisible",
    "min_quantity": "minQuantity",
    "is_block": "isBlock",
    "exclusive_group_id": "exclusiveGroupId",
}

class BidRowGraphQL(GraphQLCore):
    """This represents the reading version of bid row, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

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
        power_asset: TODO description
        alerts: An array of associated alerts.
    """
    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BidRow", "1")
    price: Optional[float] = None
    quantity_per_hour: Optional[list[float]] = Field(None, alias="quantityPerHour")
    product: Optional[str] = None
    is_divisible: Optional[bool] = Field(None, alias="isDivisible")
    min_quantity: Optional[list[float]] = Field(None, alias="minQuantity")
    is_block: Optional[bool] = Field(None, alias="isBlock")
    exclusive_group_id: Optional[str] = Field(None, alias="exclusiveGroupId")
    linked_bid: Optional[BidRowGraphQL] = Field(default=None, repr=False, alias="linkedBid")
    power_asset: Optional[PowerAssetGraphQL] = Field(default=None, repr=False, alias="powerAsset")
    alerts: Optional[list[AlertGraphQL]] = Field(default=None, repr=False)

    @model_validator(mode="before")
    def parse_data_record(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        if "lastUpdatedTime" in values or "createdTime" in values:
            values["dataRecord"] = DataRecordGraphQL(
                created_time=values.pop("createdTime", None),
                last_updated_time=values.pop("lastUpdatedTime", None),
            )
        return values
    @field_validator("linked_bid", "power_asset", "alerts", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> BidRow:
        """Convert this GraphQL format of bid row to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return BidRow(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            price=self.price,
            quantity_per_hour=self.quantity_per_hour,
            product=self.product,
            is_divisible=self.is_divisible,
            min_quantity=self.min_quantity,
            is_block=self.is_block,
            exclusive_group_id=self.exclusive_group_id,
            linked_bid=self.linked_bid.as_read() if isinstance(self.linked_bid, GraphQLCore) else self.linked_bid,
            power_asset=self.power_asset.as_read() if isinstance(self.power_asset, GraphQLCore) else self.power_asset,
            alerts=[alert.as_read() for alert in self.alerts or []],
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BidRowWrite:
        """Convert this GraphQL format of bid row to the writing format."""
        return BidRowWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            price=self.price,
            quantity_per_hour=self.quantity_per_hour,
            product=self.product,
            is_divisible=self.is_divisible,
            min_quantity=self.min_quantity,
            is_block=self.is_block,
            exclusive_group_id=self.exclusive_group_id,
            linked_bid=self.linked_bid.as_write() if isinstance(self.linked_bid, GraphQLCore) else self.linked_bid,
            power_asset=self.power_asset.as_write() if isinstance(self.power_asset, GraphQLCore) else self.power_asset,
            alerts=[alert.as_write() for alert in self.alerts or []],
        )


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
        power_asset: TODO description
        alerts: An array of associated alerts.
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BidRow", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "BidRow")
    price: Optional[float] = None
    quantity_per_hour: Optional[list[float]] = Field(None, alias="quantityPerHour")
    product: Optional[str] = None
    is_divisible: Optional[bool] = Field(None, alias="isDivisible")
    min_quantity: Optional[list[float]] = Field(None, alias="minQuantity")
    is_block: Optional[bool] = Field(None, alias="isBlock")
    exclusive_group_id: Optional[str] = Field(None, alias="exclusiveGroupId")
    linked_bid: Union[BidRow, str, dm.NodeId, None] = Field(default=None, repr=False, alias="linkedBid")
    power_asset: Union[PowerAsset, str, dm.NodeId, None] = Field(default=None, repr=False, alias="powerAsset")
    alerts: Optional[list[Union[Alert, str, dm.NodeId]]] = Field(default=None, repr=False)

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
            power_asset=self.power_asset.as_write() if isinstance(self.power_asset, DomainModel) else self.power_asset,
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
        power_asset: TODO description
        alerts: An array of associated alerts.
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BidRow", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "BidRow")
    price: Optional[float] = None
    quantity_per_hour: Optional[list[float]] = Field(None, alias="quantityPerHour")
    product: Optional[str] = None
    is_divisible: Optional[bool] = Field(None, alias="isDivisible")
    min_quantity: Optional[list[float]] = Field(None, alias="minQuantity")
    is_block: Optional[bool] = Field(None, alias="isBlock")
    exclusive_group_id: Optional[str] = Field(None, alias="exclusiveGroupId")
    linked_bid: Union[BidRowWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="linkedBid")
    power_asset: Union[PowerAssetWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="powerAsset")
    alerts: Optional[list[Union[AlertWrite, str, dm.NodeId]]] = Field(default=None, repr=False)

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

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
                "space":  self.space if isinstance(self.linked_bid, str) else self.linked_bid.space,
                "externalId": self.linked_bid if isinstance(self.linked_bid, str) else self.linked_bid.external_id,
            }

        if self.power_asset is not None:
            properties["powerAsset"] = {
                "space":  self.space if isinstance(self.power_asset, str) else self.power_asset.space,
                "externalId": self.power_asset if isinstance(self.power_asset, str) else self.power_asset.external_id,
            }


        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                )],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())



        edge_type = dm.DirectRelationReference("power_ops_types", "calculationIssue")
        for alert in self.alerts or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=alert,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.linked_bid, DomainModelWrite):
            other_resources = self.linked_bid._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.power_asset, DomainModelWrite):
            other_resources = self.power_asset._to_instances_write(cache)
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
    power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
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
        filters.append(dm.filters.Equals(view_id.as_property_ref("linkedBid"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": linked_bid}))
    if linked_bid and isinstance(linked_bid, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("linkedBid"), value={"space": linked_bid[0], "externalId": linked_bid[1]}))
    if linked_bid and isinstance(linked_bid, list) and isinstance(linked_bid[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("linkedBid"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in linked_bid]))
    if linked_bid and isinstance(linked_bid, list) and isinstance(linked_bid[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("linkedBid"), values=[{"space": item[0], "externalId": item[1]} for item in linked_bid]))
    if power_asset and isinstance(power_asset, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("powerAsset"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": power_asset}))
    if power_asset and isinstance(power_asset, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("powerAsset"), value={"space": power_asset[0], "externalId": power_asset[1]}))
    if power_asset and isinstance(power_asset, list) and isinstance(power_asset[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("powerAsset"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in power_asset]))
    if power_asset and isinstance(power_asset, list) and isinstance(power_asset[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("powerAsset"), values=[{"space": item[0], "externalId": item[1]} for item in power_asset]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
