from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
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
    TimeSeries,
)

if TYPE_CHECKING:
    from ._bid_method import BidMethod, BidMethodWrite


__all__ = [
    "PriceArea",
    "PriceAreaWrite",
    "PriceAreaApply",
    "PriceAreaList",
    "PriceAreaWriteList",
    "PriceAreaApplyList",
    "PriceAreaFields",
    "PriceAreaTextFields",
]


PriceAreaTextFields = Literal["name", "timezone", "main_scenario", "price_scenarios"]
PriceAreaFields = Literal["name", "timezone", "main_scenario", "price_scenarios"]

_PRICEAREA_PROPERTIES_BY_FIELD = {
    "name": "name",
    "timezone": "timezone",
    "main_scenario": "mainScenario",
    "price_scenarios": "priceScenarios",
}


class PriceArea(DomainModel):
    """This represents the reading version of price area.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area.
        data_record: The data record of the price area node.
        name: The name field.
        default_method: The default method field.
        timezone: The timezone field.
        main_scenario: The main scenario field.
        price_scenarios: The price scenario field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power-ops-types", "PriceArea")
    name: str
    default_method: Union[BidMethod, str, dm.NodeId, None] = Field(None, repr=False, alias="defaultMethod")
    timezone: str
    main_scenario: Union[TimeSeries, str, None] = Field(None, alias="mainScenario")
    price_scenarios: Union[list[TimeSeries], list[str], None] = Field(None, alias="priceScenarios")

    def as_write(self) -> PriceAreaWrite:
        """Convert this read version of price area to the writing version."""
        return PriceAreaWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            default_method=(
                self.default_method.as_write() if isinstance(self.default_method, DomainModel) else self.default_method
            ),
            timezone=self.timezone,
            main_scenario=self.main_scenario,
            price_scenarios=self.price_scenarios,
        )

    def as_apply(self) -> PriceAreaWrite:
        """Convert this read version of price area to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PriceAreaWrite(DomainModelWrite):
    """This represents the writing version of price area.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area.
        data_record: The data record of the price area node.
        name: The name field.
        default_method: The default method field.
        timezone: The timezone field.
        main_scenario: The main scenario field.
        price_scenarios: The price scenario field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power-ops-types", "PriceArea")
    name: str
    default_method: Union[BidMethodWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="defaultMethod")
    timezone: str
    main_scenario: Union[TimeSeries, str, None] = Field(None, alias="mainScenario")
    price_scenarios: Union[list[TimeSeries], list[str], None] = Field(None, alias="priceScenarios")

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(PriceArea, dm.ViewId("power-ops-day-ahead-bid", "PriceArea", "1"))

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.default_method is not None:
            properties["defaultMethod"] = {
                "space": self.space if isinstance(self.default_method, str) else self.default_method.space,
                "externalId": (
                    self.default_method if isinstance(self.default_method, str) else self.default_method.external_id
                ),
            }

        if self.timezone is not None:
            properties["timezone"] = self.timezone

        if self.main_scenario is not None or write_none:
            if isinstance(self.main_scenario, str) or self.main_scenario is None:
                properties["mainScenario"] = self.main_scenario
            else:
                properties["mainScenario"] = self.main_scenario.external_id

        if self.price_scenarios is not None or write_none:
            properties["priceScenarios"] = [
                value if isinstance(value, str) else value.external_id for value in self.price_scenarios or []
            ] or None

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

        if isinstance(self.default_method, DomainModelWrite):
            other_resources = self.default_method._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.main_scenario, CogniteTimeSeries):
            resources.time_series.append(self.main_scenario)

        if isinstance(self.price_scenarios, CogniteTimeSeries):
            resources.time_series.append(self.price_scenarios)

        return resources


class PriceAreaApply(PriceAreaWrite):
    def __new__(cls, *args, **kwargs) -> PriceAreaApply:
        warnings.warn(
            "PriceAreaApply is deprecated and will be removed in v1.0. Use PriceAreaWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PriceArea.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class PriceAreaList(DomainModelList[PriceArea]):
    """List of price areas in the read version."""

    _INSTANCE = PriceArea

    def as_write(self) -> PriceAreaWriteList:
        """Convert these read versions of price area to the writing versions."""
        return PriceAreaWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PriceAreaWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PriceAreaWriteList(DomainModelWriteList[PriceAreaWrite]):
    """List of price areas in the writing version."""

    _INSTANCE = PriceAreaWrite


class PriceAreaApplyList(PriceAreaWriteList): ...


def _create_price_area_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    default_method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    timezone: str | list[str] | None = None,
    timezone_prefix: str | None = None,
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
    if default_method and isinstance(default_method, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("defaultMethod"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": default_method},
            )
        )
    if default_method and isinstance(default_method, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("defaultMethod"),
                value={"space": default_method[0], "externalId": default_method[1]},
            )
        )
    if default_method and isinstance(default_method, list) and isinstance(default_method[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("defaultMethod"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in default_method],
            )
        )
    if default_method and isinstance(default_method, list) and isinstance(default_method[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("defaultMethod"),
                values=[{"space": item[0], "externalId": item[1]} for item in default_method],
            )
        )
    if isinstance(timezone, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timezone"), value=timezone))
    if timezone and isinstance(timezone, list):
        filters.append(dm.filters.In(view_id.as_property_ref("timezone"), values=timezone))
    if timezone_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("timezone"), value=timezone_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
