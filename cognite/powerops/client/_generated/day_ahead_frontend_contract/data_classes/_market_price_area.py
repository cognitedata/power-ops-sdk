from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import Field

from ._core import (
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
    "MarketPriceArea",
    "MarketPriceAreaApply",
    "MarketPriceAreaList",
    "MarketPriceAreaApplyList",
    "MarketPriceAreaFields",
    "MarketPriceAreaTextFields",
]


MarketPriceAreaTextFields = Literal["name", "price_area", "timezone"]
MarketPriceAreaFields = Literal["name", "price_area", "timezone", "main_scenario", "price_scenarios"]

_MARKETPRICEAREA_PROPERTIES_BY_FIELD = {
    "name": "name",
    "price_area": "priceArea",
    "timezone": "timezone",
    "main_scenario": "mainScenario",
    "price_scenarios": "priceScenarios",
}


class MarketPriceArea(DomainModel):
    """This represents the reading version of market price area.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the market price area.
        name: The name field.
        price_area: The price area field.
        default_method: The default method field.
        timezone: The timezone field.
        main_scenario: The main scenario field.
        price_scenarios: The price scenario field.
        created_time: The created time of the market price area node.
        last_updated_time: The last updated time of the market price area node.
        deleted_time: If present, the deleted time of the market price area node.
        version: The version of the market price area node.
    """

    space: str = "poweropsDayAheadFrontendContractModel"
    name: Optional[str] = None
    price_area: Optional[str] = Field(None, alias="priceArea")
    default_method: Union[BidMethod, str, None] = Field(None, repr=False, alias="defaultMethod")
    timezone: Optional[str] = None
    main_scenario: Union[TimeSeries, str, None] = Field(None, alias="mainScenario")
    price_scenarios: Optional[list[TimeSeries]] = Field(None, alias="priceScenarios")

    def as_apply(self) -> MarketPriceAreaApply:
        """Convert this read version of market price area to the writing version."""
        return MarketPriceAreaApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            price_area=self.price_area,
            default_method=self.default_method.as_apply()
            if isinstance(self.default_method, DomainModel)
            else self.default_method,
            timezone=self.timezone,
            main_scenario=self.main_scenario,
            price_scenarios=self.price_scenarios,
        )


class MarketPriceAreaApply(DomainModelApply):
    """This represents the writing version of market price area.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the market price area.
        name: The name field.
        price_area: The price area field.
        default_method: The default method field.
        timezone: The timezone field.
        main_scenario: The main scenario field.
        price_scenarios: The price scenario field.
        existing_version: Fail the ingestion request if the market price area version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "poweropsDayAheadFrontendContractModel"
    name: Optional[str] = None
    price_area: Optional[str] = Field(None, alias="priceArea")
    default_method: Union[BidMethodApply, str, None] = Field(None, repr=False, alias="defaultMethod")
    timezone: Optional[str] = None
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
            "poweropsDayAheadFrontendContractModel", "MarketPriceArea", "1"
        )

        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.price_area is not None:
            properties["priceArea"] = self.price_area
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

        return resources


class MarketPriceAreaList(DomainModelList[MarketPriceArea]):
    """List of market price areas in the read version."""

    _INSTANCE = MarketPriceArea

    def as_apply(self) -> MarketPriceAreaApplyList:
        """Convert these read versions of market price area to the writing versions."""
        return MarketPriceAreaApplyList([node.as_apply() for node in self.data])


class MarketPriceAreaApplyList(DomainModelApplyList[MarketPriceAreaApply]):
    """List of market price areas in the writing version."""

    _INSTANCE = MarketPriceAreaApply


def _create_market_price_area_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    price_area: str | list[str] | None = None,
    price_area_prefix: str | None = None,
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
    if price_area and isinstance(price_area, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("priceArea"), value=price_area))
    if price_area and isinstance(price_area, list):
        filters.append(dm.filters.In(view_id.as_property_ref("priceArea"), values=price_area))
    if price_area_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("priceArea"), value=price_area_prefix))
    if default_method and isinstance(default_method, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("defaultMethod"),
                value={"space": "poweropsDayAheadFrontendContractModel", "externalId": default_method},
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
                values=[
                    {"space": "poweropsDayAheadFrontendContractModel", "externalId": item} for item in default_method
                ],
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
