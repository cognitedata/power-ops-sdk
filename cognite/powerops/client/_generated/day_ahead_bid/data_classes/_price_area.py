from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    TimeSeries,
)

if TYPE_CHECKING:
    from ._bid_method import BidMethod, BidMethodApply


__all__ = [
    "PriceArea",
    "PriceAreaApply",
    "PriceAreaList",
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
        name: The name field.
        default_method: The default method field.
        timezone: The timezone field.
        main_scenario: The main scenario field.
        price_scenarios: The price scenario field.
        created_time: The created time of the price area node.
        last_updated_time: The last updated time of the price area node.
        deleted_time: If present, the deleted time of the price area node.
        version: The version of the price area node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    name: Optional[str] = None
    default_method: Union[BidMethod, str, dm.NodeId, None] = Field(None, repr=False, alias="defaultMethod")
    timezone: Optional[str] = None
    main_scenario: Union[TimeSeries, str, None] = Field(None, alias="mainScenario")
    price_scenarios: Optional[list[TimeSeries]] = Field(None, alias="priceScenarios")

    def as_apply(self) -> PriceAreaApply:
        """Convert this read version of price area to the writing version."""
        return PriceAreaApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            default_method=self.default_method.as_apply()
            if isinstance(self.default_method, DomainModel)
            else self.default_method,
            timezone=self.timezone,
            main_scenario=self.main_scenario,
            price_scenarios=self.price_scenarios,
        )


class PriceAreaApply(DomainModelApply):
    """This represents the writing version of price area.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area.
        name: The name field.
        default_method: The default method field.
        timezone: The timezone field.
        main_scenario: The main scenario field.
        price_scenarios: The price scenario field.
        existing_version: Fail the ingestion request if the price area version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    name: str
    default_method: Union[BidMethodApply, str, dm.NodeId, None] = Field(None, repr=False, alias="defaultMethod")
    timezone: str
    main_scenario: Union[TimeSeries, str, None] = Field(None, alias="mainScenario")
    price_scenarios: Optional[list[TimeSeries]] = Field(None, alias="priceScenarios")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "power-ops-day-ahead-bid", "PriceArea", "1"
        )

        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.default_method is not None:
            properties["defaultMethod"] = {
                "space": self.space if isinstance(self.default_method, str) else self.default_method.space,
                "externalId": self.default_method
                if isinstance(self.default_method, str)
                else self.default_method.external_id,
            }
        if self.timezone is not None:
            properties["timezone"] = self.timezone
        if self.main_scenario is not None:
            properties["mainScenario"] = (
                self.main_scenario if isinstance(self.main_scenario, str) else self.main_scenario.external_id
            )
        if self.price_scenarios is not None:
            properties["priceScenarios"] = self.price_scenarios

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.default_method, DomainModelApply):
            other_resources = self.default_method._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        if isinstance(self.main_scenario, CogniteTimeSeries):
            resources.time_series.append(self.main_scenario)

        if isinstance(self.price_scenarios, CogniteTimeSeries):
            resources.time_series.append(self.price_scenarios)

        return resources


class PriceAreaList(DomainModelList[PriceArea]):
    """List of price areas in the read version."""

    _INSTANCE = PriceArea

    def as_apply(self) -> PriceAreaApplyList:
        """Convert these read versions of price area to the writing versions."""
        return PriceAreaApplyList([node.as_apply() for node in self.data])


class PriceAreaApplyList(DomainModelApplyList[PriceAreaApply]):
    """List of price areas in the writing version."""

    _INSTANCE = PriceAreaApply


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
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if default_method and isinstance(default_method, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("defaultMethod"),
                value={"space": "power-ops-day-ahead-bid", "externalId": default_method},
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
                values=[{"space": "power-ops-day-ahead-bid", "externalId": item} for item in default_method],
            )
        )
    if default_method and isinstance(default_method, list) and isinstance(default_method[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("defaultMethod"),
                values=[{"space": item[0], "externalId": item[1]} for item in default_method],
            )
        )
    if timezone and isinstance(timezone, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timezone"), value=timezone))
    if timezone and isinstance(timezone, list):
        filters.append(dm.filters.In(view_id.as_property_ref("timezone"), values=timezone))
    if timezone_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("timezone"), value=timezone_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
