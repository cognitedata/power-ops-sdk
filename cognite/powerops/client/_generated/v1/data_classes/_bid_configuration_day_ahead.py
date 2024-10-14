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
    from ._date_specification import DateSpecification, DateSpecificationGraphQL, DateSpecificationWrite
    from ._market_configuration import MarketConfiguration, MarketConfigurationGraphQL, MarketConfigurationWrite
    from ._partial_bid_configuration import PartialBidConfiguration, PartialBidConfigurationGraphQL, PartialBidConfigurationWrite
    from ._price_area_day_ahead import PriceAreaDayAhead, PriceAreaDayAheadGraphQL, PriceAreaDayAheadWrite


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


BidConfigurationDayAheadTextFields = Literal["name"]
BidConfigurationDayAheadFields = Literal["name"]

_BIDCONFIGURATIONDAYAHEAD_PROPERTIES_BY_FIELD = {
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
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            market_configuration=self.market_configuration.as_read() if isinstance(self.market_configuration, GraphQLCore) else self.market_configuration,
            price_area=self.price_area.as_read() if isinstance(self.price_area, GraphQLCore) else self.price_area,
            bid_date_specification=self.bid_date_specification.as_read() if isinstance(self.bid_date_specification, GraphQLCore) else self.bid_date_specification,
            partials=[partial.as_read() for partial in self.partials or []],
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BidConfigurationDayAheadWrite:
        """Convert this GraphQL format of bid configuration day ahead to the writing format."""
        return BidConfigurationDayAheadWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            market_configuration=self.market_configuration.as_write() if isinstance(self.market_configuration, GraphQLCore) else self.market_configuration,
            price_area=self.price_area.as_write() if isinstance(self.price_area, GraphQLCore) else self.price_area,
            bid_date_specification=self.bid_date_specification.as_write() if isinstance(self.bid_date_specification, GraphQLCore) else self.bid_date_specification,
            partials=[partial.as_write() for partial in self.partials or []],
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

    def as_write(self) -> BidConfigurationDayAheadWrite:
        """Convert this read version of bid configuration day ahead to the writing version."""
        return BidConfigurationDayAheadWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            market_configuration=self.market_configuration.as_write() if isinstance(self.market_configuration, DomainModel) else self.market_configuration,
            price_area=self.price_area.as_write() if isinstance(self.price_area, DomainModel) else self.price_area,
            bid_date_specification=self.bid_date_specification.as_write() if isinstance(self.bid_date_specification, DomainModel) else self.bid_date_specification,
            partials=[partial.as_write() if isinstance(partial, DomainModel) else partial for partial in self.partials or []],
        )

    def as_apply(self) -> BidConfigurationDayAheadWrite:
        """Convert this read version of bid configuration day ahead to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


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
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "BidConfigurationDayAhead")
    name: str
    market_configuration: Union[MarketConfigurationWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="marketConfiguration")
    price_area: Union[PriceAreaDayAheadWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="priceArea")
    bid_date_specification: Union[DateSpecificationWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="bidDateSpecification")
    partials: Optional[list[Union[PartialBidConfigurationWrite, str, dm.NodeId]]] = Field(default=None, repr=False)

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
                type=self.node_type,
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


class BidConfigurationDayAheadWriteList(DomainModelWriteList[BidConfigurationDayAheadWrite]):
    """List of bid configuration day aheads in the writing version."""

    _INSTANCE = BidConfigurationDayAheadWrite

class BidConfigurationDayAheadApplyList(BidConfigurationDayAheadWriteList): ...



def _create_bid_configuration_day_ahead_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    market_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    bid_date_specification: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if market_configuration and isinstance(market_configuration, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("marketConfiguration"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": market_configuration}))
    if market_configuration and isinstance(market_configuration, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("marketConfiguration"), value={"space": market_configuration[0], "externalId": market_configuration[1]}))
    if market_configuration and isinstance(market_configuration, list) and isinstance(market_configuration[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("marketConfiguration"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in market_configuration]))
    if market_configuration and isinstance(market_configuration, list) and isinstance(market_configuration[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("marketConfiguration"), values=[{"space": item[0], "externalId": item[1]} for item in market_configuration]))
    if price_area and isinstance(price_area, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("priceArea"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": price_area}))
    if price_area and isinstance(price_area, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("priceArea"), value={"space": price_area[0], "externalId": price_area[1]}))
    if price_area and isinstance(price_area, list) and isinstance(price_area[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("priceArea"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in price_area]))
    if price_area and isinstance(price_area, list) and isinstance(price_area[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("priceArea"), values=[{"space": item[0], "externalId": item[1]} for item in price_area]))
    if bid_date_specification and isinstance(bid_date_specification, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bidDateSpecification"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": bid_date_specification}))
    if bid_date_specification and isinstance(bid_date_specification, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bidDateSpecification"), value={"space": bid_date_specification[0], "externalId": bid_date_specification[1]}))
    if bid_date_specification and isinstance(bid_date_specification, list) and isinstance(bid_date_specification[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("bidDateSpecification"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in bid_date_specification]))
    if bid_date_specification and isinstance(bid_date_specification, list) and isinstance(bid_date_specification[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("bidDateSpecification"), values=[{"space": item[0], "externalId": item[1]} for item in bid_date_specification]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
