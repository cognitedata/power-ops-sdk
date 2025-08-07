from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

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
    FloatFilter,
    IntFilter,
)


__all__ = [
    "MarketConfiguration",
    "MarketConfigurationWrite",
    "MarketConfigurationList",
    "MarketConfigurationWriteList",
    "MarketConfigurationFields",
    "MarketConfigurationTextFields",
    "MarketConfigurationGraphQL",
]


MarketConfigurationTextFields = Literal["external_id", "name", "timezone", "price_unit", "time_unit"]
MarketConfigurationFields = Literal["external_id", "name", "max_price", "min_price", "timezone", "price_unit", "price_steps", "tick_size", "time_unit", "trade_lot"]

_MARKETCONFIGURATION_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
    "max_price": "maxPrice",
    "min_price": "minPrice",
    "timezone": "timezone",
    "price_unit": "priceUnit",
    "price_steps": "priceSteps",
    "tick_size": "tickSize",
    "time_unit": "timeUnit",
    "trade_lot": "tradeLot",
}


class MarketConfigurationGraphQL(GraphQLCore):
    """This represents the reading version of market configuration, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the market configuration.
        data_record: The data record of the market configuration node.
        name: The name of the market
        max_price: The highest price allowed
        min_price: The lowest price allowed
        timezone: The timezone field.
        price_unit: Unit of measurement for the price ('EUR/MWh')
        price_steps: The maximum number of price steps
        tick_size: 'Granularity' of the price; tick size = 0.1 means that prices must be 'rounded to nearest 0.1' (i.
            e. 66.43 is not allowed, but 66.4 is)
        time_unit: The time unit ('1h')
        trade_lot: 'Granularity' of the volumes; trade lot = 0.2 means that volumes must be 'rounded to nearest 0.2'
            (i. e. 66.5 is not allowed, but 66.4 is)
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "MarketConfiguration", "1")
    name: Optional[str] = None
    max_price: Optional[float] = Field(None, alias="maxPrice")
    min_price: Optional[float] = Field(None, alias="minPrice")
    timezone: Optional[str] = None
    price_unit: Optional[str] = Field(None, alias="priceUnit")
    price_steps: Optional[int] = Field(None, alias="priceSteps")
    tick_size: Optional[float] = Field(None, alias="tickSize")
    time_unit: Optional[str] = Field(None, alias="timeUnit")
    trade_lot: Optional[float] = Field(None, alias="tradeLot")

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



    def as_read(self) -> MarketConfiguration:
        """Convert this GraphQL format of market configuration to the reading format."""
        return MarketConfiguration.model_validate(as_read_args(self))

    def as_write(self) -> MarketConfigurationWrite:
        """Convert this GraphQL format of market configuration to the writing format."""
        return MarketConfigurationWrite.model_validate(as_write_args(self))


class MarketConfiguration(DomainModel):
    """This represents the reading version of market configuration.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the market configuration.
        data_record: The data record of the market configuration node.
        name: The name of the market
        max_price: The highest price allowed
        min_price: The lowest price allowed
        timezone: The timezone field.
        price_unit: Unit of measurement for the price ('EUR/MWh')
        price_steps: The maximum number of price steps
        tick_size: 'Granularity' of the price; tick size = 0.1 means that prices must be 'rounded to nearest 0.1' (i.
            e. 66.43 is not allowed, but 66.4 is)
        time_unit: The time unit ('1h')
        trade_lot: 'Granularity' of the volumes; trade lot = 0.2 means that volumes must be 'rounded to nearest 0.2'
            (i. e. 66.5 is not allowed, but 66.4 is)
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "MarketConfiguration", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "MarketConfiguration")
    name: str
    max_price: float = Field(alias="maxPrice")
    min_price: float = Field(alias="minPrice")
    timezone: str
    price_unit: str = Field(alias="priceUnit")
    price_steps: int = Field(alias="priceSteps")
    tick_size: float = Field(alias="tickSize")
    time_unit: str = Field(alias="timeUnit")
    trade_lot: float = Field(alias="tradeLot")


    def as_write(self) -> MarketConfigurationWrite:
        """Convert this read version of market configuration to the writing version."""
        return MarketConfigurationWrite.model_validate(as_write_args(self))



class MarketConfigurationWrite(DomainModelWrite):
    """This represents the writing version of market configuration.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the market configuration.
        data_record: The data record of the market configuration node.
        name: The name of the market
        max_price: The highest price allowed
        min_price: The lowest price allowed
        timezone: The timezone field.
        price_unit: Unit of measurement for the price ('EUR/MWh')
        price_steps: The maximum number of price steps
        tick_size: 'Granularity' of the price; tick size = 0.1 means that prices must be 'rounded to nearest 0.1' (i.
            e. 66.43 is not allowed, but 66.4 is)
        time_unit: The time unit ('1h')
        trade_lot: 'Granularity' of the volumes; trade lot = 0.2 means that volumes must be 'rounded to nearest 0.2'
            (i. e. 66.5 is not allowed, but 66.4 is)
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("max_price", "min_price", "name", "price_steps", "price_unit", "tick_size", "time_unit", "timezone", "trade_lot",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "MarketConfiguration", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "MarketConfiguration")
    name: str
    max_price: float = Field(alias="maxPrice")
    min_price: float = Field(alias="minPrice")
    timezone: str
    price_unit: str = Field(alias="priceUnit")
    price_steps: int = Field(alias="priceSteps")
    tick_size: float = Field(alias="tickSize")
    time_unit: str = Field(alias="timeUnit")
    trade_lot: float = Field(alias="tradeLot")



class MarketConfigurationList(DomainModelList[MarketConfiguration]):
    """List of market configurations in the read version."""

    _INSTANCE = MarketConfiguration
    def as_write(self) -> MarketConfigurationWriteList:
        """Convert these read versions of market configuration to the writing versions."""
        return MarketConfigurationWriteList([node.as_write() for node in self.data])



class MarketConfigurationWriteList(DomainModelWriteList[MarketConfigurationWrite]):
    """List of market configurations in the writing version."""

    _INSTANCE = MarketConfigurationWrite


def _create_market_configuration_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    min_max_price: float | None = None,
    max_max_price: float | None = None,
    min_min_price: float | None = None,
    max_min_price: float | None = None,
    timezone: str | list[str] | None = None,
    timezone_prefix: str | None = None,
    price_unit: str | list[str] | None = None,
    price_unit_prefix: str | None = None,
    min_price_steps: int | None = None,
    max_price_steps: int | None = None,
    min_tick_size: float | None = None,
    max_tick_size: float | None = None,
    time_unit: str | list[str] | None = None,
    time_unit_prefix: str | None = None,
    min_trade_lot: float | None = None,
    max_trade_lot: float | None = None,
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
    if min_max_price is not None or max_max_price is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("maxPrice"), gte=min_max_price, lte=max_max_price))
    if min_min_price is not None or max_min_price is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("minPrice"), gte=min_min_price, lte=max_min_price))
    if isinstance(timezone, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timezone"), value=timezone))
    if timezone and isinstance(timezone, list):
        filters.append(dm.filters.In(view_id.as_property_ref("timezone"), values=timezone))
    if timezone_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("timezone"), value=timezone_prefix))
    if isinstance(price_unit, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("priceUnit"), value=price_unit))
    if price_unit and isinstance(price_unit, list):
        filters.append(dm.filters.In(view_id.as_property_ref("priceUnit"), values=price_unit))
    if price_unit_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("priceUnit"), value=price_unit_prefix))
    if min_price_steps is not None or max_price_steps is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("priceSteps"), gte=min_price_steps, lte=max_price_steps))
    if min_tick_size is not None or max_tick_size is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("tickSize"), gte=min_tick_size, lte=max_tick_size))
    if isinstance(time_unit, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timeUnit"), value=time_unit))
    if time_unit and isinstance(time_unit, list):
        filters.append(dm.filters.In(view_id.as_property_ref("timeUnit"), values=time_unit))
    if time_unit_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("timeUnit"), value=time_unit_prefix))
    if min_trade_lot is not None or max_trade_lot is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("tradeLot"), gte=min_trade_lot, lte=max_trade_lot))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _MarketConfigurationQuery(NodeQueryCore[T_DomainModelList, MarketConfigurationList]):
    _view_id = MarketConfiguration._view_id
    _result_cls = MarketConfiguration
    _result_list_cls_end = MarketConfigurationList

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

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.max_price = FloatFilter(self, self._view_id.as_property_ref("maxPrice"))
        self.min_price = FloatFilter(self, self._view_id.as_property_ref("minPrice"))
        self.timezone = StringFilter(self, self._view_id.as_property_ref("timezone"))
        self.price_unit = StringFilter(self, self._view_id.as_property_ref("priceUnit"))
        self.price_steps = IntFilter(self, self._view_id.as_property_ref("priceSteps"))
        self.tick_size = FloatFilter(self, self._view_id.as_property_ref("tickSize"))
        self.time_unit = StringFilter(self, self._view_id.as_property_ref("timeUnit"))
        self.trade_lot = FloatFilter(self, self._view_id.as_property_ref("tradeLot"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.max_price,
            self.min_price,
            self.timezone,
            self.price_unit,
            self.price_steps,
            self.tick_size,
            self.time_unit,
            self.trade_lot,
        ])

    def list_market_configuration(self, limit: int = DEFAULT_QUERY_LIMIT) -> MarketConfigurationList:
        return self._list(limit=limit)


class MarketConfigurationQuery(_MarketConfigurationQuery[MarketConfigurationList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, MarketConfigurationList)
