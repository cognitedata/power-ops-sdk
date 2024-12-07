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

)
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._date_specification import DateSpecification, DateSpecificationList, DateSpecificationGraphQL, DateSpecificationWrite, DateSpecificationWriteList
    from cognite.powerops.client._generated.v1.data_classes._market_configuration import MarketConfiguration, MarketConfigurationList, MarketConfigurationGraphQL, MarketConfigurationWrite, MarketConfigurationWriteList
    from cognite.powerops.client._generated.v1.data_classes._partial_bid_configuration import PartialBidConfiguration, PartialBidConfigurationList, PartialBidConfigurationGraphQL, PartialBidConfigurationWrite, PartialBidConfigurationWriteList
    from cognite.powerops.client._generated.v1.data_classes._price_area_day_ahead import PriceAreaDayAhead, PriceAreaDayAheadList, PriceAreaDayAheadGraphQL, PriceAreaDayAheadWrite, PriceAreaDayAheadWriteList


__all__ = [
    "BidConfigurationDayAhead",
    "BidConfigurationDayAheadWrite",
    "BidConfigurationDayAheadApply",
    "BidConfigurationDayAheadList",
    "BidConfigurationDayAheadWriteList",
    "BidConfigurationDayAheadApplyList",
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> BidConfigurationDayAhead:
        """Convert this GraphQL format of bid configuration day ahead to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return BidConfigurationDayAhead(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            market_configuration=self.market_configuration.as_read()
if isinstance(self.market_configuration, GraphQLCore)
else self.market_configuration,
            price_area=self.price_area.as_read()
if isinstance(self.price_area, GraphQLCore)
else self.price_area,
            bid_date_specification=self.bid_date_specification.as_read()
if isinstance(self.bid_date_specification, GraphQLCore)
else self.bid_date_specification,
            partials=[partial.as_read() for partial in self.partials] if self.partials is not None else None,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BidConfigurationDayAheadWrite:
        """Convert this GraphQL format of bid configuration day ahead to the writing format."""
        return BidConfigurationDayAheadWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            market_configuration=self.market_configuration.as_write()
if isinstance(self.market_configuration, GraphQLCore)
else self.market_configuration,
            price_area=self.price_area.as_write()
if isinstance(self.price_area, GraphQLCore)
else self.price_area,
            bid_date_specification=self.bid_date_specification.as_write()
if isinstance(self.bid_date_specification, GraphQLCore)
else self.bid_date_specification,
            partials=[partial.as_write() for partial in self.partials] if self.partials is not None else None,
        )


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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BidConfigurationDayAheadWrite:
        """Convert this read version of bid configuration day ahead to the writing version."""
        return BidConfigurationDayAheadWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            market_configuration=self.market_configuration.as_write()
if isinstance(self.market_configuration, DomainModel)
else self.market_configuration,
            price_area=self.price_area.as_write()
if isinstance(self.price_area, DomainModel)
else self.price_area,
            bid_date_specification=self.bid_date_specification.as_write()
if isinstance(self.bid_date_specification, DomainModel)
else self.bid_date_specification,
            partials=[partial.as_write() if isinstance(partial, DomainModel) else partial for partial in self.partials] if self.partials is not None else None,
        )

    def as_apply(self) -> BidConfigurationDayAheadWrite:
        """Convert this read version of bid configuration day ahead to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()
    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, BidConfigurationDayAhead],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._date_specification import DateSpecification
        from ._market_configuration import MarketConfiguration
        from ._partial_bid_configuration import PartialBidConfiguration
        from ._price_area_day_ahead import PriceAreaDayAhead
        for instance in instances.values():
            if isinstance(instance.market_configuration, (dm.NodeId, str)) and (market_configuration := nodes_by_id.get(instance.market_configuration)) and isinstance(
                    market_configuration, MarketConfiguration
            ):
                instance.market_configuration = market_configuration
            if isinstance(instance.price_area, (dm.NodeId, str)) and (price_area := nodes_by_id.get(instance.price_area)) and isinstance(
                    price_area, PriceAreaDayAhead
            ):
                instance.price_area = price_area
            if isinstance(instance.bid_date_specification, (dm.NodeId, str)) and (bid_date_specification := nodes_by_id.get(instance.bid_date_specification)) and isinstance(
                    bid_date_specification, DateSpecification
            ):
                instance.bid_date_specification = bid_date_specification
            if edges := edges_by_source_node.get(instance.as_id()):
                partials: list[PartialBidConfiguration | str | dm.NodeId] = []
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

                    if edge_type == dm.DirectRelationReference("power_ops_types", "BidConfiguration.partials") and isinstance(
                        value, (PartialBidConfiguration, str, dm.NodeId)
                    ):
                        partials.append(value)

                instance.partials = partials or None



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

        if self.name is not None:
            properties["name"] = self.name

        if self.market_configuration is not None:
            properties["marketConfiguration"] = {
                "space":  self.space if isinstance(self.market_configuration, str) else self.market_configuration.space,
                "externalId": self.market_configuration if isinstance(self.market_configuration, str) else self.market_configuration.external_id,
            }

        if self.price_area is not None:
            properties["priceArea"] = {
                "space":  self.space if isinstance(self.price_area, str) else self.price_area.space,
                "externalId": self.price_area if isinstance(self.price_area, str) else self.price_area.external_id,
            }

        if self.bid_date_specification is not None:
            properties["bidDateSpecification"] = {
                "space":  self.space if isinstance(self.bid_date_specification, str) else self.bid_date_specification.space,
                "externalId": self.bid_date_specification if isinstance(self.bid_date_specification, str) else self.bid_date_specification.external_id,
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

        edge_type = dm.DirectRelationReference("power_ops_types", "BidConfiguration.partials")
        for partial in self.partials or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=partial,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.market_configuration, DomainModelWrite):
            other_resources = self.market_configuration._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.price_area, DomainModelWrite):
            other_resources = self.price_area._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.bid_date_specification, DomainModelWrite):
            other_resources = self.bid_date_specification._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class BidConfigurationDayAheadApply(BidConfigurationDayAheadWrite):
    def __new__(cls, *args, **kwargs) -> BidConfigurationDayAheadApply:
        warnings.warn(
            "BidConfigurationDayAheadApply is deprecated and will be removed in v1.0. Use BidConfigurationDayAheadWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "BidConfigurationDayAhead.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)

class BidConfigurationDayAheadList(DomainModelList[BidConfigurationDayAhead]):
    """List of bid configuration day aheads in the read version."""

    _INSTANCE = BidConfigurationDayAhead
    def as_write(self) -> BidConfigurationDayAheadWriteList:
        """Convert these read versions of bid configuration day ahead to the writing versions."""
        return BidConfigurationDayAheadWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> BidConfigurationDayAheadWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

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


class BidConfigurationDayAheadApplyList(BidConfigurationDayAheadWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
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
            connection_type,
            reverse_expression,
        )

        if _MarketConfigurationQuery not in created_types:
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
            )

        if _PriceAreaDayAheadQuery not in created_types:
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
            )

        if _DateSpecificationQuery not in created_types:
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
            )

        if _PartialBidConfigurationQuery not in created_types:
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
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
        ])

    def list_bid_configuration_day_ahead(self, limit: int = DEFAULT_QUERY_LIMIT) -> BidConfigurationDayAheadList:
        return self._list(limit=limit)


class BidConfigurationDayAheadQuery(_BidConfigurationDayAheadQuery[BidConfigurationDayAheadList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, BidConfigurationDayAheadList)
