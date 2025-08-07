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
    DirectRelationFilter,
)
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._date_specification import DateSpecification, DateSpecificationList, DateSpecificationGraphQL, DateSpecificationWrite, DateSpecificationWriteList
    from cognite.powerops.client._generated.v1.data_classes._market_configuration import MarketConfiguration, MarketConfigurationList, MarketConfigurationGraphQL, MarketConfigurationWrite, MarketConfigurationWriteList
    from cognite.powerops.client._generated.v1.data_classes._partial_bid_configuration import PartialBidConfiguration, PartialBidConfigurationList, PartialBidConfigurationGraphQL, PartialBidConfigurationWrite, PartialBidConfigurationWriteList
    from cognite.powerops.client._generated.v1.data_classes._price_area_day_ahead import PriceAreaDayAhead, PriceAreaDayAheadList, PriceAreaDayAheadGraphQL, PriceAreaDayAheadWrite, PriceAreaDayAheadWriteList


__all__ = [
    "BidConfigurationDayAhead",
    "BidConfigurationDayAheadWrite",
    "BidConfigurationDayAheadList",
    "BidConfigurationDayAheadWriteList",
    "BidConfigurationDayAheadFields",
    "BidConfigurationDayAheadTextFields",
    "BidConfigurationDayAheadGraphQL",
]


BidConfigurationDayAheadTextFields = Literal["external_id", "name"]
BidConfigurationDayAheadFields = Literal["external_id", "name"]

_BIDCONFIGURATIONDAYAHEAD_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
}


class BidConfigurationDayAheadGraphQL(GraphQLCore):
    """This represents the reading version of bid configuration day ahead, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid configuration day ahead.
        data_record: The data record of the bid configuration day ahead node.
        name: The name of the bid configuration
        market_configuration: The market configuration related to the bid configuration
        price_area: The price area related to the bid calculation task
        bid_date_specification: TODO description
        partials: Configuration of the partial bids that make up the total bid
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BidConfigurationDayAhead", "1")
    name: Optional[str] = None
    market_configuration: Optional[MarketConfigurationGraphQL] = Field(default=None, repr=False, alias="marketConfiguration")
    price_area: Optional[PriceAreaDayAheadGraphQL] = Field(default=None, repr=False, alias="priceArea")
    bid_date_specification: Optional[DateSpecificationGraphQL] = Field(default=None, repr=False, alias="bidDateSpecification")
    partials: Optional[list[PartialBidConfigurationGraphQL]] = Field(default=None, repr=False)

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


    @field_validator("market_configuration", "price_area", "bid_date_specification", "partials", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> BidConfigurationDayAhead:
        """Convert this GraphQL format of bid configuration day ahead to the reading format."""
        return BidConfigurationDayAhead.model_validate(as_read_args(self))

    def as_write(self) -> BidConfigurationDayAheadWrite:
        """Convert this GraphQL format of bid configuration day ahead to the writing format."""
        return BidConfigurationDayAheadWrite.model_validate(as_write_args(self))


class BidConfigurationDayAhead(DomainModel):
    """This represents the reading version of bid configuration day ahead.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid configuration day ahead.
        data_record: The data record of the bid configuration day ahead node.
        name: The name of the bid configuration
        market_configuration: The market configuration related to the bid configuration
        price_area: The price area related to the bid calculation task
        bid_date_specification: TODO description
        partials: Configuration of the partial bids that make up the total bid
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BidConfigurationDayAhead", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "BidConfigurationDayAhead")
    name: str
    market_configuration: Union[MarketConfiguration, str, dm.NodeId, None] = Field(default=None, repr=False, alias="marketConfiguration")
    price_area: Union[PriceAreaDayAhead, str, dm.NodeId, None] = Field(default=None, repr=False, alias="priceArea")
    bid_date_specification: Union[DateSpecification, str, dm.NodeId, None] = Field(default=None, repr=False, alias="bidDateSpecification")
    partials: Optional[list[Union[PartialBidConfiguration, str, dm.NodeId]]] = Field(default=None, repr=False)
    @field_validator("market_configuration", "price_area", "bid_date_specification", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("partials", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> BidConfigurationDayAheadWrite:
        """Convert this read version of bid configuration day ahead to the writing version."""
        return BidConfigurationDayAheadWrite.model_validate(as_write_args(self))



class BidConfigurationDayAheadWrite(DomainModelWrite):
    """This represents the writing version of bid configuration day ahead.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid configuration day ahead.
        data_record: The data record of the bid configuration day ahead node.
        name: The name of the bid configuration
        market_configuration: The market configuration related to the bid configuration
        price_area: The price area related to the bid calculation task
        bid_date_specification: TODO description
        partials: Configuration of the partial bids that make up the total bid
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("bid_date_specification", "market_configuration", "name", "price_area",)
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (("partials", dm.DirectRelationReference("power_ops_types", "BidConfiguration.partials")),)
    _direct_relations: ClassVar[tuple[str, ...]] = ("bid_date_specification", "market_configuration", "price_area",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BidConfigurationDayAhead", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "BidConfigurationDayAhead")
    name: str
    market_configuration: Union[MarketConfigurationWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="marketConfiguration")
    price_area: Union[PriceAreaDayAheadWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="priceArea")
    bid_date_specification: Union[DateSpecificationWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="bidDateSpecification")
    partials: Optional[list[Union[PartialBidConfigurationWrite, str, dm.NodeId]]] = Field(default=None, repr=False)

    @field_validator("market_configuration", "price_area", "bid_date_specification", "partials", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class BidConfigurationDayAheadList(DomainModelList[BidConfigurationDayAhead]):
    """List of bid configuration day aheads in the read version."""

    _INSTANCE = BidConfigurationDayAhead
    def as_write(self) -> BidConfigurationDayAheadWriteList:
        """Convert these read versions of bid configuration day ahead to the writing versions."""
        return BidConfigurationDayAheadWriteList([node.as_write() for node in self.data])


    @property
    def market_configuration(self) -> MarketConfigurationList:
        from ._market_configuration import MarketConfiguration, MarketConfigurationList
        return MarketConfigurationList([item.market_configuration for item in self.data if isinstance(item.market_configuration, MarketConfiguration)])
    @property
    def price_area(self) -> PriceAreaDayAheadList:
        from ._price_area_day_ahead import PriceAreaDayAhead, PriceAreaDayAheadList
        return PriceAreaDayAheadList([item.price_area for item in self.data if isinstance(item.price_area, PriceAreaDayAhead)])
    @property
    def bid_date_specification(self) -> DateSpecificationList:
        from ._date_specification import DateSpecification, DateSpecificationList
        return DateSpecificationList([item.bid_date_specification for item in self.data if isinstance(item.bid_date_specification, DateSpecification)])
    @property
    def partials(self) -> PartialBidConfigurationList:
        from ._partial_bid_configuration import PartialBidConfiguration, PartialBidConfigurationList
        return PartialBidConfigurationList([item for items in self.data for item in items.partials or [] if isinstance(item, PartialBidConfiguration)])


class BidConfigurationDayAheadWriteList(DomainModelWriteList[BidConfigurationDayAheadWrite]):
    """List of bid configuration day aheads in the writing version."""

    _INSTANCE = BidConfigurationDayAheadWrite
    @property
    def market_configuration(self) -> MarketConfigurationWriteList:
        from ._market_configuration import MarketConfigurationWrite, MarketConfigurationWriteList
        return MarketConfigurationWriteList([item.market_configuration for item in self.data if isinstance(item.market_configuration, MarketConfigurationWrite)])
    @property
    def price_area(self) -> PriceAreaDayAheadWriteList:
        from ._price_area_day_ahead import PriceAreaDayAheadWrite, PriceAreaDayAheadWriteList
        return PriceAreaDayAheadWriteList([item.price_area for item in self.data if isinstance(item.price_area, PriceAreaDayAheadWrite)])
    @property
    def bid_date_specification(self) -> DateSpecificationWriteList:
        from ._date_specification import DateSpecificationWrite, DateSpecificationWriteList
        return DateSpecificationWriteList([item.bid_date_specification for item in self.data if isinstance(item.bid_date_specification, DateSpecificationWrite)])
    @property
    def partials(self) -> PartialBidConfigurationWriteList:
        from ._partial_bid_configuration import PartialBidConfigurationWrite, PartialBidConfigurationWriteList
        return PartialBidConfigurationWriteList([item for items in self.data for item in items.partials or [] if isinstance(item, PartialBidConfigurationWrite)])



def _create_bid_configuration_day_ahead_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    market_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    price_area: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    bid_date_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if isinstance(market_configuration, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(market_configuration):
        filters.append(dm.filters.Equals(view_id.as_property_ref("marketConfiguration"), value=as_instance_dict_id(market_configuration)))
    if market_configuration and isinstance(market_configuration, Sequence) and not isinstance(market_configuration, str) and not is_tuple_id(market_configuration):
        filters.append(dm.filters.In(view_id.as_property_ref("marketConfiguration"), values=[as_instance_dict_id(item) for item in market_configuration]))
    if isinstance(price_area, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(price_area):
        filters.append(dm.filters.Equals(view_id.as_property_ref("priceArea"), value=as_instance_dict_id(price_area)))
    if price_area and isinstance(price_area, Sequence) and not isinstance(price_area, str) and not is_tuple_id(price_area):
        filters.append(dm.filters.In(view_id.as_property_ref("priceArea"), values=[as_instance_dict_id(item) for item in price_area]))
    if isinstance(bid_date_specification, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(bid_date_specification):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bidDateSpecification"), value=as_instance_dict_id(bid_date_specification)))
    if bid_date_specification and isinstance(bid_date_specification, Sequence) and not isinstance(bid_date_specification, str) and not is_tuple_id(bid_date_specification):
        filters.append(dm.filters.In(view_id.as_property_ref("bidDateSpecification"), values=[as_instance_dict_id(item) for item in bid_date_specification]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _BidConfigurationDayAheadQuery(NodeQueryCore[T_DomainModelList, BidConfigurationDayAheadList]):
    _view_id = BidConfigurationDayAhead._view_id
    _result_cls = BidConfigurationDayAhead
    _result_list_cls_end = BidConfigurationDayAheadList

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
        from ._date_specification import _DateSpecificationQuery
        from ._market_configuration import _MarketConfigurationQuery
        from ._partial_bid_configuration import _PartialBidConfigurationQuery
        from ._price_area_day_ahead import _PriceAreaDayAheadQuery

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

        if _MarketConfigurationQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.market_configuration = _MarketConfigurationQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("marketConfiguration"),
                    direction="outwards",
                ),
                connection_name="market_configuration",
                connection_property=ViewPropertyId(self._view_id, "marketConfiguration"),
            )

        if _PriceAreaDayAheadQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.price_area = _PriceAreaDayAheadQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("priceArea"),
                    direction="outwards",
                ),
                connection_name="price_area",
                connection_property=ViewPropertyId(self._view_id, "priceArea"),
            )

        if _DateSpecificationQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.bid_date_specification = _DateSpecificationQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("bidDateSpecification"),
                    direction="outwards",
                ),
                connection_name="bid_date_specification",
                connection_property=ViewPropertyId(self._view_id, "bidDateSpecification"),
            )

        if _PartialBidConfigurationQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.partials = _PartialBidConfigurationQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="partials",
                connection_property=ViewPropertyId(self._view_id, "partials"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.market_configuration_filter = DirectRelationFilter(self, self._view_id.as_property_ref("marketConfiguration"))
        self.price_area_filter = DirectRelationFilter(self, self._view_id.as_property_ref("priceArea"))
        self.bid_date_specification_filter = DirectRelationFilter(self, self._view_id.as_property_ref("bidDateSpecification"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.market_configuration_filter,
            self.price_area_filter,
            self.bid_date_specification_filter,
        ])

    def list_bid_configuration_day_ahead(self, limit: int = DEFAULT_QUERY_LIMIT) -> BidConfigurationDayAheadList:
        return self._list(limit=limit)


class BidConfigurationDayAheadQuery(_BidConfigurationDayAheadQuery[BidConfigurationDayAheadList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, BidConfigurationDayAheadList)
