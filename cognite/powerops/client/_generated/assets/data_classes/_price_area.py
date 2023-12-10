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
    from ._plant import Plant, PlantApply
    from ._watercourse import Watercourse, WatercourseApply


__all__ = [
    "PriceArea",
    "PriceAreaApply",
    "PriceAreaList",
    "PriceAreaApplyList",
    "PriceAreaFields",
    "PriceAreaTextFields",
]


PriceAreaTextFields = Literal[
    "name",
    "display_name",
    "description",
    "timezone",
    "capacity_price_up",
    "capacity_price_down",
    "activation_price_up",
    "activation_price_down",
    "relative_activation",
    "total_capacity_allocation_up",
    "total_capacity_allocation_down",
    "own_capacity_allocation_up",
    "own_capacity_allocation_down",
    "main_scenario_day_ahead",
    "day_ahead_price",
]
PriceAreaFields = Literal[
    "name",
    "display_name",
    "description",
    "timezone",
    "capacity_price_up",
    "capacity_price_down",
    "activation_price_up",
    "activation_price_down",
    "relative_activation",
    "total_capacity_allocation_up",
    "total_capacity_allocation_down",
    "own_capacity_allocation_up",
    "own_capacity_allocation_down",
    "main_scenario_day_ahead",
    "day_ahead_price",
]

_PRICEAREA_PROPERTIES_BY_FIELD = {
    "name": "name",
    "display_name": "displayName",
    "description": "description",
    "timezone": "timezone",
    "capacity_price_up": "capacityPriceUp",
    "capacity_price_down": "capacityPriceDown",
    "activation_price_up": "activationPriceUp",
    "activation_price_down": "activationPriceDown",
    "relative_activation": "relativeActivation",
    "total_capacity_allocation_up": "totalCapacityAllocationUp",
    "total_capacity_allocation_down": "totalCapacityAllocationDown",
    "own_capacity_allocation_up": "ownCapacityAllocationUp",
    "own_capacity_allocation_down": "ownCapacityAllocationDown",
    "main_scenario_day_ahead": "mainScenarioDayAhead",
    "day_ahead_price": "dayAheadPrice",
}


class PriceArea(DomainModel):
    """This represents the reading version of price area.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area.
        name: Name for the PriceArea.
        display_name: Display name for the PriceArea.
        description: Description for the PriceArea.
        timezone: The timezone of the price area
        capacity_price_up: The capacity price up field.
        capacity_price_down: The capacity price down field.
        activation_price_up: The mFRR activation price (TBC)
        activation_price_down: The mFRR activate price (TBC)
        relative_activation: Value between -1 (100 % activation down) and 1 (100 % activation down)
        total_capacity_allocation_up: The total capacity allocation up field.
        total_capacity_allocation_down: The total capacity allocation down field.
        own_capacity_allocation_up: The own capacity allocation up field.
        own_capacity_allocation_down: The own capacity allocation down field.
        default_method_day_ahead: Default method for day ahead bids
        main_scenario_day_ahead: Main scenario for day ahead bids
        day_ahead_price: Day ahead price for the price area
        plants: The plants that are connected to the Watercourse.
        watercourses: The watercourses that are connected to the PriceArea.
        created_time: The created time of the price area node.
        last_updated_time: The last updated time of the price area node.
        deleted_time: If present, the deleted time of the price area node.
        version: The version of the price area node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    name: Optional[str] = None
    display_name: Optional[str] = Field(None, alias="displayName")
    description: Optional[str] = None
    timezone: Optional[str] = None
    capacity_price_up: Union[TimeSeries, str, None] = Field(None, alias="capacityPriceUp")
    capacity_price_down: Union[TimeSeries, str, None] = Field(None, alias="capacityPriceDown")
    activation_price_up: Union[TimeSeries, str, None] = Field(None, alias="activationPriceUp")
    activation_price_down: Union[TimeSeries, str, None] = Field(None, alias="activationPriceDown")
    relative_activation: Union[TimeSeries, str, None] = Field(None, alias="relativeActivation")
    total_capacity_allocation_up: Union[TimeSeries, str, None] = Field(None, alias="totalCapacityAllocationUp")
    total_capacity_allocation_down: Union[TimeSeries, str, None] = Field(None, alias="totalCapacityAllocationDown")
    own_capacity_allocation_up: Union[TimeSeries, str, None] = Field(None, alias="ownCapacityAllocationUp")
    own_capacity_allocation_down: Union[TimeSeries, str, None] = Field(None, alias="ownCapacityAllocationDown")
    default_method_day_ahead: Union[BidMethod, str, dm.NodeId, None] = Field(
        None, repr=False, alias="defaultMethodDayAhead"
    )
    main_scenario_day_ahead: Union[TimeSeries, str, None] = Field(None, alias="mainScenarioDayAhead")
    day_ahead_price: Union[TimeSeries, str, None] = Field(None, alias="dayAheadPrice")
    plants: Union[list[Plant], list[str], None] = Field(default=None, repr=False)
    watercourses: Union[list[Watercourse], list[str], None] = Field(default=None, repr=False)

    def as_apply(self) -> PriceAreaApply:
        """Convert this read version of price area to the writing version."""
        return PriceAreaApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            display_name=self.display_name,
            description=self.description,
            timezone=self.timezone,
            capacity_price_up=self.capacity_price_up,
            capacity_price_down=self.capacity_price_down,
            activation_price_up=self.activation_price_up,
            activation_price_down=self.activation_price_down,
            relative_activation=self.relative_activation,
            total_capacity_allocation_up=self.total_capacity_allocation_up,
            total_capacity_allocation_down=self.total_capacity_allocation_down,
            own_capacity_allocation_up=self.own_capacity_allocation_up,
            own_capacity_allocation_down=self.own_capacity_allocation_down,
            default_method_day_ahead=self.default_method_day_ahead.as_apply()
            if isinstance(self.default_method_day_ahead, DomainModel)
            else self.default_method_day_ahead,
            main_scenario_day_ahead=self.main_scenario_day_ahead,
            day_ahead_price=self.day_ahead_price,
            plants=[plant.as_apply() if isinstance(plant, DomainModel) else plant for plant in self.plants or []],
            watercourses=[
                watercourse.as_apply() if isinstance(watercourse, DomainModel) else watercourse
                for watercourse in self.watercourses or []
            ],
        )


class PriceAreaApply(DomainModelApply):
    """This represents the writing version of price area.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area.
        name: Name for the PriceArea.
        display_name: Display name for the PriceArea.
        description: Description for the PriceArea.
        timezone: The timezone of the price area
        capacity_price_up: The capacity price up field.
        capacity_price_down: The capacity price down field.
        activation_price_up: The mFRR activation price (TBC)
        activation_price_down: The mFRR activate price (TBC)
        relative_activation: Value between -1 (100 % activation down) and 1 (100 % activation down)
        total_capacity_allocation_up: The total capacity allocation up field.
        total_capacity_allocation_down: The total capacity allocation down field.
        own_capacity_allocation_up: The own capacity allocation up field.
        own_capacity_allocation_down: The own capacity allocation down field.
        default_method_day_ahead: Default method for day ahead bids
        main_scenario_day_ahead: Main scenario for day ahead bids
        day_ahead_price: Day ahead price for the price area
        plants: The plants that are connected to the Watercourse.
        watercourses: The watercourses that are connected to the PriceArea.
        existing_version: Fail the ingestion request if the price area version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    name: str
    display_name: Optional[str] = Field(None, alias="displayName")
    description: Optional[str] = None
    timezone: str
    capacity_price_up: Union[TimeSeries, str, None] = Field(None, alias="capacityPriceUp")
    capacity_price_down: Union[TimeSeries, str, None] = Field(None, alias="capacityPriceDown")
    activation_price_up: Union[TimeSeries, str, None] = Field(None, alias="activationPriceUp")
    activation_price_down: Union[TimeSeries, str, None] = Field(None, alias="activationPriceDown")
    relative_activation: Union[TimeSeries, str, None] = Field(None, alias="relativeActivation")
    total_capacity_allocation_up: Union[TimeSeries, str, None] = Field(None, alias="totalCapacityAllocationUp")
    total_capacity_allocation_down: Union[TimeSeries, str, None] = Field(None, alias="totalCapacityAllocationDown")
    own_capacity_allocation_up: Union[TimeSeries, str, None] = Field(None, alias="ownCapacityAllocationUp")
    own_capacity_allocation_down: Union[TimeSeries, str, None] = Field(None, alias="ownCapacityAllocationDown")
    default_method_day_ahead: Union[BidMethodApply, str, dm.NodeId, None] = Field(
        None, repr=False, alias="defaultMethodDayAhead"
    )
    main_scenario_day_ahead: Union[TimeSeries, str, None] = Field(None, alias="mainScenarioDayAhead")
    day_ahead_price: Union[TimeSeries, str, None] = Field(None, alias="dayAheadPrice")
    plants: Union[list[PlantApply], list[str], None] = Field(default=None, repr=False)
    watercourses: Union[list[WatercourseApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "power-ops-assets", "PriceArea", "1"
        )

        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.display_name is not None:
            properties["displayName"] = self.display_name
        if self.description is not None:
            properties["description"] = self.description
        if self.timezone is not None:
            properties["timezone"] = self.timezone
        if self.capacity_price_up is not None:
            properties["capacityPriceUp"] = (
                self.capacity_price_up
                if isinstance(self.capacity_price_up, str)
                else self.capacity_price_up.external_id
            )
        if self.capacity_price_down is not None:
            properties["capacityPriceDown"] = (
                self.capacity_price_down
                if isinstance(self.capacity_price_down, str)
                else self.capacity_price_down.external_id
            )
        if self.activation_price_up is not None:
            properties["activationPriceUp"] = (
                self.activation_price_up
                if isinstance(self.activation_price_up, str)
                else self.activation_price_up.external_id
            )
        if self.activation_price_down is not None:
            properties["activationPriceDown"] = (
                self.activation_price_down
                if isinstance(self.activation_price_down, str)
                else self.activation_price_down.external_id
            )
        if self.relative_activation is not None:
            properties["relativeActivation"] = (
                self.relative_activation
                if isinstance(self.relative_activation, str)
                else self.relative_activation.external_id
            )
        if self.total_capacity_allocation_up is not None:
            properties["totalCapacityAllocationUp"] = (
                self.total_capacity_allocation_up
                if isinstance(self.total_capacity_allocation_up, str)
                else self.total_capacity_allocation_up.external_id
            )
        if self.total_capacity_allocation_down is not None:
            properties["totalCapacityAllocationDown"] = (
                self.total_capacity_allocation_down
                if isinstance(self.total_capacity_allocation_down, str)
                else self.total_capacity_allocation_down.external_id
            )
        if self.own_capacity_allocation_up is not None:
            properties["ownCapacityAllocationUp"] = (
                self.own_capacity_allocation_up
                if isinstance(self.own_capacity_allocation_up, str)
                else self.own_capacity_allocation_up.external_id
            )
        if self.own_capacity_allocation_down is not None:
            properties["ownCapacityAllocationDown"] = (
                self.own_capacity_allocation_down
                if isinstance(self.own_capacity_allocation_down, str)
                else self.own_capacity_allocation_down.external_id
            )
        if self.default_method_day_ahead is not None:
            properties["defaultMethodDayAhead"] = {
                "space": self.space
                if isinstance(self.default_method_day_ahead, str)
                else self.default_method_day_ahead.space,
                "externalId": self.default_method_day_ahead
                if isinstance(self.default_method_day_ahead, str)
                else self.default_method_day_ahead.external_id,
            }
        if self.main_scenario_day_ahead is not None:
            properties["mainScenarioDayAhead"] = (
                self.main_scenario_day_ahead
                if isinstance(self.main_scenario_day_ahead, str)
                else self.main_scenario_day_ahead.external_id
            )
        if self.day_ahead_price is not None:
            properties["dayAheadPrice"] = (
                self.day_ahead_price if isinstance(self.day_ahead_price, str) else self.day_ahead_price.external_id
            )

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

        edge_type = dm.DirectRelationReference("power-ops-types", "isSubAssetOf")
        for plant in self.plants or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, plant, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("power-ops-types", "isSubAssetOf")
        for watercourse in self.watercourses or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, watercourse, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        if isinstance(self.default_method_day_ahead, DomainModelApply):
            other_resources = self.default_method_day_ahead._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

        if isinstance(self.capacity_price_up, CogniteTimeSeries):
            resources.time_series.append(self.capacity_price_up)

        if isinstance(self.capacity_price_down, CogniteTimeSeries):
            resources.time_series.append(self.capacity_price_down)

        if isinstance(self.activation_price_up, CogniteTimeSeries):
            resources.time_series.append(self.activation_price_up)

        if isinstance(self.activation_price_down, CogniteTimeSeries):
            resources.time_series.append(self.activation_price_down)

        if isinstance(self.relative_activation, CogniteTimeSeries):
            resources.time_series.append(self.relative_activation)

        if isinstance(self.total_capacity_allocation_up, CogniteTimeSeries):
            resources.time_series.append(self.total_capacity_allocation_up)

        if isinstance(self.total_capacity_allocation_down, CogniteTimeSeries):
            resources.time_series.append(self.total_capacity_allocation_down)

        if isinstance(self.own_capacity_allocation_up, CogniteTimeSeries):
            resources.time_series.append(self.own_capacity_allocation_up)

        if isinstance(self.own_capacity_allocation_down, CogniteTimeSeries):
            resources.time_series.append(self.own_capacity_allocation_down)

        if isinstance(self.main_scenario_day_ahead, CogniteTimeSeries):
            resources.time_series.append(self.main_scenario_day_ahead)

        if isinstance(self.day_ahead_price, CogniteTimeSeries):
            resources.time_series.append(self.day_ahead_price)

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
    display_name: str | list[str] | None = None,
    display_name_prefix: str | None = None,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    timezone: str | list[str] | None = None,
    timezone_prefix: str | None = None,
    default_method_day_ahead: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if display_name and isinstance(display_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("displayName"), value=display_name))
    if display_name and isinstance(display_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("displayName"), values=display_name))
    if display_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("displayName"), value=display_name_prefix))
    if description and isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if timezone and isinstance(timezone, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timezone"), value=timezone))
    if timezone and isinstance(timezone, list):
        filters.append(dm.filters.In(view_id.as_property_ref("timezone"), values=timezone))
    if timezone_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("timezone"), value=timezone_prefix))
    if default_method_day_ahead and isinstance(default_method_day_ahead, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("defaultMethodDayAhead"),
                value={"space": "power-ops-shared", "externalId": default_method_day_ahead},
            )
        )
    if default_method_day_ahead and isinstance(default_method_day_ahead, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("defaultMethodDayAhead"),
                value={"space": default_method_day_ahead[0], "externalId": default_method_day_ahead[1]},
            )
        )
    if (
        default_method_day_ahead
        and isinstance(default_method_day_ahead, list)
        and isinstance(default_method_day_ahead[0], str)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("defaultMethodDayAhead"),
                values=[{"space": "power-ops-shared", "externalId": item} for item in default_method_day_ahead],
            )
        )
    if (
        default_method_day_ahead
        and isinstance(default_method_day_ahead, list)
        and isinstance(default_method_day_ahead[0], tuple)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("defaultMethodDayAhead"),
                values=[{"space": item[0], "externalId": item[1]} for item in default_method_day_ahead],
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
