from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

from cognite.powerops.client._generated.v1.config import global_config
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
    as_node_id,
    as_read_args,
    as_write_args,
    is_tuple_id,
    as_instance_dict_id,
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    ViewPropertyId,
    BooleanFilter,
    DirectRelationFilter,
    FloatFilter,
)
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._alert import Alert, AlertList, AlertGraphQL, AlertWrite, AlertWriteList
    from cognite.powerops.client._generated.v1.data_classes._power_asset import PowerAsset, PowerAssetList, PowerAssetGraphQL, PowerAssetWrite, PowerAssetWriteList


__all__ = [
    "BidRow",
    "BidRowWrite",
    "BidRowList",
    "BidRowWriteList",
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
        min_quantity: Min quantity, per hour. Only relevant for divisible Bids. The minimum capacity that must be
            accepted; this must be lower than capacityPerHour and is rounded to the nearest step (5 MW?)).
        is_block: Indication if the row is part of a Block bid. If true: quantityPerHour must have the same value for
            consecutive hours (and no breaks). Block bids must be accepted for all hours or none.
        exclusive_group_id: Other bids with the same ID are part of an exclusive group - only one of them can be
            accepted, and they must have the same direction (product). Not allowed for block bids.
        linked_bid: The linked bid must have the opposite direction (link means that both or none must be accepted).
            Should be bi-directional.
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

    def as_read(self) -> BidRow:
        """Convert this GraphQL format of bid row to the reading format."""
        return BidRow.model_validate(as_read_args(self))

    def as_write(self) -> BidRowWrite:
        """Convert this GraphQL format of bid row to the writing format."""
        return BidRowWrite.model_validate(as_write_args(self))


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
        min_quantity: Min quantity, per hour. Only relevant for divisible Bids. The minimum capacity that must be
            accepted; this must be lower than capacityPerHour and is rounded to the nearest step (5 MW?)).
        is_block: Indication if the row is part of a Block bid. If true: quantityPerHour must have the same value for
            consecutive hours (and no breaks). Block bids must be accepted for all hours or none.
        exclusive_group_id: Other bids with the same ID are part of an exclusive group - only one of them can be
            accepted, and they must have the same direction (product). Not allowed for block bids.
        linked_bid: The linked bid must have the opposite direction (link means that both or none must be accepted).
            Should be bi-directional.
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
    @field_validator("linked_bid", "power_asset", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("alerts", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> BidRowWrite:
        """Convert this read version of bid row to the writing version."""
        return BidRowWrite.model_validate(as_write_args(self))



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
        min_quantity: Min quantity, per hour. Only relevant for divisible Bids. The minimum capacity that must be
            accepted; this must be lower than capacityPerHour and is rounded to the nearest step (5 MW?)).
        is_block: Indication if the row is part of a Block bid. If true: quantityPerHour must have the same value for
            consecutive hours (and no breaks). Block bids must be accepted for all hours or none.
        exclusive_group_id: Other bids with the same ID are part of an exclusive group - only one of them can be
            accepted, and they must have the same direction (product). Not allowed for block bids.
        linked_bid: The linked bid must have the opposite direction (link means that both or none must be accepted).
            Should be bi-directional.
        power_asset: TODO description
        alerts: An array of associated alerts.
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("exclusive_group_id", "is_block", "is_divisible", "linked_bid", "min_quantity", "power_asset", "price", "product", "quantity_per_hour",)
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (("alerts", dm.DirectRelationReference("power_ops_types", "calculationIssue")),)
    _direct_relations: ClassVar[tuple[str, ...]] = ("linked_bid", "power_asset",)

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


class BidRowList(DomainModelList[BidRow]):
    """List of bid rows in the read version."""

    _INSTANCE = BidRow
    def as_write(self) -> BidRowWriteList:
        """Convert these read versions of bid row to the writing versions."""
        return BidRowWriteList([node.as_write() for node in self.data])


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
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
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
            connection_property,
            connection_type,
            reverse_expression,
        )

        if _BidRowQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "linkedBid"),
            )

        if _PowerAssetQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "powerAsset"),
            )

        if _AlertQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "alerts"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.price = FloatFilter(self, self._view_id.as_property_ref("price"))
        self.product = StringFilter(self, self._view_id.as_property_ref("product"))
        self.is_divisible = BooleanFilter(self, self._view_id.as_property_ref("isDivisible"))
        self.is_block = BooleanFilter(self, self._view_id.as_property_ref("isBlock"))
        self.exclusive_group_id = StringFilter(self, self._view_id.as_property_ref("exclusiveGroupId"))
        self.linked_bid_filter = DirectRelationFilter(self, self._view_id.as_property_ref("linkedBid"))
        self.power_asset_filter = DirectRelationFilter(self, self._view_id.as_property_ref("powerAsset"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.price,
            self.product,
            self.is_divisible,
            self.is_block,
            self.exclusive_group_id,
            self.linked_bid_filter,
            self.power_asset_filter,
        ])

    def list_bid_row(self, limit: int = DEFAULT_QUERY_LIMIT) -> BidRowList:
        return self._list(limit=limit)


class BidRowQuery(_BidRowQuery[BidRowList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, BidRowList)
