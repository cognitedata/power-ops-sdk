from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal,  no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator

from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelation,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    T_DomainModelList,
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    is_tuple_id,
    select_best_node,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    BooleanFilter,
    FloatFilter,
)
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._alert import Alert, AlertList, AlertGraphQL, AlertWrite, AlertWriteList
    from cognite.powerops.client._generated.v1.data_classes._power_asset import PowerAsset, PowerAssetList, PowerAssetGraphQL, PowerAssetWrite, PowerAssetWriteList


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


BidRowTextFields = Literal["external_id", "product", "exclusive_group_id"]
BidRowFields = Literal["external_id", "price", "quantity_per_hour", "product", "is_divisible", "min_quantity", "is_block", "exclusive_group_id"]

_BIDROW_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
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
            space=self.space,
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
            linked_bid=self.linked_bid.as_read()
if isinstance(self.linked_bid, GraphQLCore)
else self.linked_bid,
            power_asset=self.power_asset.as_read()
if isinstance(self.power_asset, GraphQLCore)
else self.power_asset,
            alerts=[alert.as_read() for alert in self.alerts] if self.alerts is not None else None,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BidRowWrite:
        """Convert this GraphQL format of bid row to the writing format."""
        return BidRowWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            price=self.price,
            quantity_per_hour=self.quantity_per_hour,
            product=self.product,
            is_divisible=self.is_divisible,
            min_quantity=self.min_quantity,
            is_block=self.is_block,
            exclusive_group_id=self.exclusive_group_id,
            linked_bid=self.linked_bid.as_write()
if isinstance(self.linked_bid, GraphQLCore)
else self.linked_bid,
            power_asset=self.power_asset.as_write()
if isinstance(self.power_asset, GraphQLCore)
else self.power_asset,
            alerts=[alert.as_write() for alert in self.alerts] if self.alerts is not None else None,
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
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
            linked_bid=self.linked_bid.as_write()
if isinstance(self.linked_bid, DomainModel)
else self.linked_bid,
            power_asset=self.power_asset.as_write()
if isinstance(self.power_asset, DomainModel)
else self.power_asset,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts] if self.alerts is not None else None,
        )

    def as_apply(self) -> BidRowWrite:
        """Convert this read version of bid row to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()
    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, BidRow],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._alert import Alert
        from ._power_asset import PowerAsset
        for instance in instances.values():
            if isinstance(instance.linked_bid, (dm.NodeId, str)) and (linked_bid := nodes_by_id.get(instance.linked_bid)) and isinstance(
                    linked_bid, BidRow
            ):
                instance.linked_bid = linked_bid
            if isinstance(instance.power_asset, (dm.NodeId, str)) and (power_asset := nodes_by_id.get(instance.power_asset)) and isinstance(
                    power_asset, PowerAsset
            ):
                instance.power_asset = power_asset
            if edges := edges_by_source_node.get(instance.as_id()):
                alerts: list[Alert | str | dm.NodeId] = []
                for edge in edges:
                    value: DomainModel | DomainRelation | str | dm.NodeId
                    if isinstance(edge, DomainRelation):
                        value = edge
                    else:
                        other_end: dm.DirectRelationReference = (
                            edge.end_node
                            if edge.start_node.space == instance.space
                            and edge.start_node.external_id == instance.external_id
                            else edge.start_node
                        )
                        destination: dm.NodeId | str = (
                            as_node_id(other_end)
                            if other_end.space != DEFAULT_INSTANCE_SPACE
                            else other_end.external_id
                        )
                        if destination in nodes_by_id:
                            value = nodes_by_id[destination]
                        else:
                            value = destination
                    edge_type = edge.edge_type if isinstance(edge, DomainRelation) else edge.type

                    if edge_type == dm.DirectRelationReference("power_ops_types", "calculationIssue") and isinstance(
                        value, (Alert, str, dm.NodeId)
                    ):
                        alerts.append(value)

                instance.alerts = alerts or None



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
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "BidRow")
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

    @field_validator("linked_bid", "power_asset", "alerts", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value

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
                type=as_direct_relation_reference(self.node_type),
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

    @property
    def linked_bid(self) -> BidRowList:
        return BidRowList([item.linked_bid for item in self.data if isinstance(item.linked_bid, BidRow)])
    @property
    def power_asset(self) -> PowerAssetList:
        from ._power_asset import PowerAsset, PowerAssetList
        return PowerAssetList([item.power_asset for item in self.data if isinstance(item.power_asset, PowerAsset)])
    @property
    def alerts(self) -> AlertList:
        from ._alert import Alert, AlertList
        return AlertList([item for items in self.data for item in items.alerts or [] if isinstance(item, Alert)])


class BidRowWriteList(DomainModelWriteList[BidRowWrite]):
    """List of bid rows in the writing version."""

    _INSTANCE = BidRowWrite
    @property
    def linked_bid(self) -> BidRowWriteList:
        return BidRowWriteList([item.linked_bid for item in self.data if isinstance(item.linked_bid, BidRowWrite)])
    @property
    def power_asset(self) -> PowerAssetWriteList:
        from ._power_asset import PowerAssetWrite, PowerAssetWriteList
        return PowerAssetWriteList([item.power_asset for item in self.data if isinstance(item.power_asset, PowerAssetWrite)])
    @property
    def alerts(self) -> AlertWriteList:
        from ._alert import AlertWrite, AlertWriteList
        return AlertWriteList([item for items in self.data for item in items.alerts or [] if isinstance(item, AlertWrite)])


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
    linked_bid: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    power_asset: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
    if isinstance(linked_bid, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(linked_bid):
        filters.append(dm.filters.Equals(view_id.as_property_ref("linkedBid"), value=as_instance_dict_id(linked_bid)))
    if linked_bid and isinstance(linked_bid, Sequence) and not isinstance(linked_bid, str) and not is_tuple_id(linked_bid):
        filters.append(dm.filters.In(view_id.as_property_ref("linkedBid"), values=[as_instance_dict_id(item) for item in linked_bid]))
    if isinstance(power_asset, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(power_asset):
        filters.append(dm.filters.Equals(view_id.as_property_ref("powerAsset"), value=as_instance_dict_id(power_asset)))
    if power_asset and isinstance(power_asset, Sequence) and not isinstance(power_asset, str) and not is_tuple_id(power_asset):
        filters.append(dm.filters.In(view_id.as_property_ref("powerAsset"), values=[as_instance_dict_id(item) for item in power_asset]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _BidRowQuery(NodeQueryCore[T_DomainModelList, BidRowList]):
    _view_id = BidRow._view_id
    _result_cls = BidRow
    _result_list_cls_end = BidRowList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
    ):
        from ._alert import _AlertQuery
        from ._power_asset import _PowerAssetQuery

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
            connection_type,
            reverse_expression,
        )

        if _BidRowQuery not in created_types:
            self.linked_bid = _BidRowQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("linkedBid"),
                    direction="outwards",
                ),
                connection_name="linked_bid",
            )

        if _PowerAssetQuery not in created_types:
            self.power_asset = _PowerAssetQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("powerAsset"),
                    direction="outwards",
                ),
                connection_name="power_asset",
            )

        if _AlertQuery not in created_types:
            self.alerts = _AlertQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="alerts",
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.price = FloatFilter(self, self._view_id.as_property_ref("price"))
        self.product = StringFilter(self, self._view_id.as_property_ref("product"))
        self.is_divisible = BooleanFilter(self, self._view_id.as_property_ref("isDivisible"))
        self.is_block = BooleanFilter(self, self._view_id.as_property_ref("isBlock"))
        self.exclusive_group_id = StringFilter(self, self._view_id.as_property_ref("exclusiveGroupId"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.price,
            self.product,
            self.is_divisible,
            self.is_block,
            self.exclusive_group_id,
        ])

    def list_bid_row(self, limit: int = DEFAULT_QUERY_LIMIT) -> BidRowList:
        return self._list(limit=limit)


class BidRowQuery(_BidRowQuery[BidRowList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, BidRowList)
