from __future__ import annotations

import warnings
from typing import Any, Literal, Optional, Union

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


__all__ = [
    "MarketConfiguration",
    "MarketConfigurationWrite",
    "MarketConfigurationApply",
    "MarketConfigurationList",
    "MarketConfigurationWriteList",
    "MarketConfigurationApplyList",
    "MarketConfigurationFields",
    "MarketConfigurationTextFields",
]


MarketConfigurationTextFields = Literal[
    "market_type", "time_zone", "price_unit", "price_steps", "tick_size", "time_unit", "trade_lot"
]
MarketConfigurationFields = Literal[
    "market_type",
    "max_price",
    "min_price",
    "time_zone",
    "price_unit",
    "price_steps",
    "tick_size",
    "time_unit",
    "trade_lot",
]

_MARKETCONFIGURATION_PROPERTIES_BY_FIELD = {
    "market_type": "marketType",
    "max_price": "maxPrice",
    "min_price": "minPrice",
    "time_zone": "timeZone",
    "price_unit": "priceUnit",
    "price_steps": "priceSteps",
    "tick_size": "tickSize",
    "time_unit": "timeUnit",
    "trade_lot": "tradeLot",
}


class MarketConfiguration(DomainModel):
    """This represents the reading version of market configuration.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the market configuration.
        data_record: The data record of the market configuration node.
        market_type: The market type
        max_price: The maximum price
        min_price: The minimum price
        time_zone: The time zone
        price_unit: The price unit
        price_steps: The price steps
        tick_size: The tick size
        time_unit: The time unit
        trade_lot: The trade lot
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "MarketConfiguration"
    )
    market_type: str = Field(alias="marketType")
    max_price: float = Field(alias="maxPrice")
    min_price: float = Field(alias="minPrice")
    time_zone: str = Field(alias="timeZone")
    price_unit: str = Field(alias="priceUnit")
    price_steps: str = Field(alias="priceSteps")
    tick_size: str = Field(alias="tickSize")
    time_unit: str = Field(alias="timeUnit")
    trade_lot: str = Field(alias="tradeLot")

    def as_write(self) -> MarketConfigurationWrite:
        """Convert this read version of market configuration to the writing version."""
        return MarketConfigurationWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            market_type=self.market_type,
            max_price=self.max_price,
            min_price=self.min_price,
            time_zone=self.time_zone,
            price_unit=self.price_unit,
            price_steps=self.price_steps,
            tick_size=self.tick_size,
            time_unit=self.time_unit,
            trade_lot=self.trade_lot,
        )

    def as_apply(self) -> MarketConfigurationWrite:
        """Convert this read version of market configuration to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class MarketConfigurationWrite(DomainModelWrite):
    """This represents the writing version of market configuration.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the market configuration.
        data_record: The data record of the market configuration node.
        market_type: The market type
        max_price: The maximum price
        min_price: The minimum price
        time_zone: The time zone
        price_unit: The price unit
        price_steps: The price steps
        tick_size: The tick size
        time_unit: The time unit
        trade_lot: The trade lot
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "MarketConfiguration"
    )
    market_type: str = Field(alias="marketType")
    max_price: float = Field(alias="maxPrice")
    min_price: float = Field(alias="minPrice")
    time_zone: str = Field(alias="timeZone")
    price_unit: str = Field(alias="priceUnit")
    price_steps: str = Field(alias="priceSteps")
    tick_size: str = Field(alias="tickSize")
    time_unit: str = Field(alias="timeUnit")
    trade_lot: str = Field(alias="tradeLot")

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
            MarketConfiguration, dm.ViewId("sp_powerops_models", "MarketConfiguration", "1")
        )

        properties: dict[str, Any] = {}

        if self.market_type is not None:
            properties["marketType"] = self.market_type

        if self.max_price is not None:
            properties["maxPrice"] = self.max_price

        if self.min_price is not None:
            properties["minPrice"] = self.min_price

        if self.time_zone is not None:
            properties["timeZone"] = self.time_zone

        if self.price_unit is not None:
            properties["priceUnit"] = self.price_unit

        if self.price_steps is not None:
            properties["priceSteps"] = self.price_steps

        if self.tick_size is not None:
            properties["tickSize"] = self.tick_size

        if self.time_unit is not None:
            properties["timeUnit"] = self.time_unit

        if self.trade_lot is not None:
            properties["tradeLot"] = self.trade_lot

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

        return resources


class MarketConfigurationApply(MarketConfigurationWrite):
    def __new__(cls, *args, **kwargs) -> MarketConfigurationApply:
        warnings.warn(
            "MarketConfigurationApply is deprecated and will be removed in v1.0. Use MarketConfigurationWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "MarketConfiguration.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class MarketConfigurationList(DomainModelList[MarketConfiguration]):
    """List of market configurations in the read version."""

    _INSTANCE = MarketConfiguration

    def as_write(self) -> MarketConfigurationWriteList:
        """Convert these read versions of market configuration to the writing versions."""
        return MarketConfigurationWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> MarketConfigurationWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class MarketConfigurationWriteList(DomainModelWriteList[MarketConfigurationWrite]):
    """List of market configurations in the writing version."""

    _INSTANCE = MarketConfigurationWrite


class MarketConfigurationApplyList(MarketConfigurationWriteList): ...


def _create_market_configuration_filter(
    view_id: dm.ViewId,
    market_type: str | list[str] | None = None,
    market_type_prefix: str | None = None,
    min_max_price: float | None = None,
    max_max_price: float | None = None,
    min_min_price: float | None = None,
    max_min_price: float | None = None,
    time_zone: str | list[str] | None = None,
    time_zone_prefix: str | None = None,
    price_unit: str | list[str] | None = None,
    price_unit_prefix: str | None = None,
    price_steps: str | list[str] | None = None,
    price_steps_prefix: str | None = None,
    tick_size: str | list[str] | None = None,
    tick_size_prefix: str | None = None,
    time_unit: str | list[str] | None = None,
    time_unit_prefix: str | None = None,
    trade_lot: str | list[str] | None = None,
    trade_lot_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if isinstance(market_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("marketType"), value=market_type))
    if market_type and isinstance(market_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("marketType"), values=market_type))
    if market_type_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("marketType"), value=market_type_prefix))
    if min_max_price is not None or max_max_price is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("maxPrice"), gte=min_max_price, lte=max_max_price))
    if min_min_price is not None or max_min_price is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("minPrice"), gte=min_min_price, lte=max_min_price))
    if isinstance(time_zone, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timeZone"), value=time_zone))
    if time_zone and isinstance(time_zone, list):
        filters.append(dm.filters.In(view_id.as_property_ref("timeZone"), values=time_zone))
    if time_zone_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("timeZone"), value=time_zone_prefix))
    if isinstance(price_unit, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("priceUnit"), value=price_unit))
    if price_unit and isinstance(price_unit, list):
        filters.append(dm.filters.In(view_id.as_property_ref("priceUnit"), values=price_unit))
    if price_unit_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("priceUnit"), value=price_unit_prefix))
    if isinstance(price_steps, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("priceSteps"), value=price_steps))
    if price_steps and isinstance(price_steps, list):
        filters.append(dm.filters.In(view_id.as_property_ref("priceSteps"), values=price_steps))
    if price_steps_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("priceSteps"), value=price_steps_prefix))
    if isinstance(tick_size, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("tickSize"), value=tick_size))
    if tick_size and isinstance(tick_size, list):
        filters.append(dm.filters.In(view_id.as_property_ref("tickSize"), values=tick_size))
    if tick_size_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("tickSize"), value=tick_size_prefix))
    if isinstance(time_unit, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timeUnit"), value=time_unit))
    if time_unit and isinstance(time_unit, list):
        filters.append(dm.filters.In(view_id.as_property_ref("timeUnit"), values=time_unit))
    if time_unit_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("timeUnit"), value=time_unit_prefix))
    if isinstance(trade_lot, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("tradeLot"), value=trade_lot))
    if trade_lot and isinstance(trade_lot, list):
        filters.append(dm.filters.In(view_id.as_property_ref("tradeLot"), values=trade_lot))
    if trade_lot_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("tradeLot"), value=trade_lot_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
