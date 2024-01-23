from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
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
        data_record: The data record of the price area node.
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
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power-ops-types", "PriceArea")
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
            data_record=DataRecordWrite(existing_version=self.data_record.version),
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
        data_record: The data record of the price area node.
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
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power-ops-types", "PriceArea")
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
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(PriceArea, dm.ViewId("power-ops-assets", "PriceArea", "1"))

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.display_name is not None or write_none:
            properties["displayName"] = self.display_name

        if self.description is not None or write_none:
            properties["description"] = self.description

        if self.timezone is not None:
            properties["timezone"] = self.timezone

        if self.capacity_price_up is not None or write_none:
            if isinstance(self.capacity_price_up, str) or self.capacity_price_up is None:
                properties["capacityPriceUp"] = self.capacity_price_up
            else:
                properties["capacityPriceUp"] = self.capacity_price_up.external_id

        if self.capacity_price_down is not None or write_none:
            if isinstance(self.capacity_price_down, str) or self.capacity_price_down is None:
                properties["capacityPriceDown"] = self.capacity_price_down
            else:
                properties["capacityPriceDown"] = self.capacity_price_down.external_id

        if self.activation_price_up is not None or write_none:
            if isinstance(self.activation_price_up, str) or self.activation_price_up is None:
                properties["activationPriceUp"] = self.activation_price_up
            else:
                properties["activationPriceUp"] = self.activation_price_up.external_id

        if self.activation_price_down is not None or write_none:
            if isinstance(self.activation_price_down, str) or self.activation_price_down is None:
                properties["activationPriceDown"] = self.activation_price_down
            else:
                properties["activationPriceDown"] = self.activation_price_down.external_id

        if self.relative_activation is not None or write_none:
            if isinstance(self.relative_activation, str) or self.relative_activation is None:
                properties["relativeActivation"] = self.relative_activation
            else:
                properties["relativeActivation"] = self.relative_activation.external_id

        if self.total_capacity_allocation_up is not None or write_none:
            if isinstance(self.total_capacity_allocation_up, str) or self.total_capacity_allocation_up is None:
                properties["totalCapacityAllocationUp"] = self.total_capacity_allocation_up
            else:
                properties["totalCapacityAllocationUp"] = self.total_capacity_allocation_up.external_id

        if self.total_capacity_allocation_down is not None or write_none:
            if isinstance(self.total_capacity_allocation_down, str) or self.total_capacity_allocation_down is None:
                properties["totalCapacityAllocationDown"] = self.total_capacity_allocation_down
            else:
                properties["totalCapacityAllocationDown"] = self.total_capacity_allocation_down.external_id

        if self.own_capacity_allocation_up is not None or write_none:
            if isinstance(self.own_capacity_allocation_up, str) or self.own_capacity_allocation_up is None:
                properties["ownCapacityAllocationUp"] = self.own_capacity_allocation_up
            else:
                properties["ownCapacityAllocationUp"] = self.own_capacity_allocation_up.external_id

        if self.own_capacity_allocation_down is not None or write_none:
            if isinstance(self.own_capacity_allocation_down, str) or self.own_capacity_allocation_down is None:
                properties["ownCapacityAllocationDown"] = self.own_capacity_allocation_down
            else:
                properties["ownCapacityAllocationDown"] = self.own_capacity_allocation_down.external_id

        if self.default_method_day_ahead is not None:
            properties["defaultMethodDayAhead"] = {
                "space": self.space
                if isinstance(self.default_method_day_ahead, str)
                else self.default_method_day_ahead.space,
                "externalId": self.default_method_day_ahead
                if isinstance(self.default_method_day_ahead, str)
                else self.default_method_day_ahead.external_id,
            }

        if self.main_scenario_day_ahead is not None or write_none:
            if isinstance(self.main_scenario_day_ahead, str) or self.main_scenario_day_ahead is None:
                properties["mainScenarioDayAhead"] = self.main_scenario_day_ahead
            else:
                properties["mainScenarioDayAhead"] = self.main_scenario_day_ahead.external_id

        if self.day_ahead_price is not None or write_none:
            if isinstance(self.day_ahead_price, str) or self.day_ahead_price is None:
                properties["dayAheadPrice"] = self.day_ahead_price
            else:
                properties["dayAheadPrice"] = self.day_ahead_price.external_id

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

        edge_type = dm.DirectRelationReference("power-ops-types", "isSubAssetOf")
        for plant in self.plants or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, start_node=self, end_node=plant, edge_type=edge_type, view_by_read_class=view_by_read_class
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("power-ops-types", "isSubAssetOf")
        for watercourse in self.watercourses or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, start_node=self, end_node=watercourse, edge_type=edge_type, view_by_read_class=view_by_read_class
            )
            resources.extend(other_resources)

        if isinstance(self.default_method_day_ahead, DomainModelApply):
            other_resources = self.default_method_day_ahead._to_instances_apply(cache, view_by_read_class)
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
    if name is not None and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if display_name is not None and isinstance(display_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("displayName"), value=display_name))
    if display_name and isinstance(display_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("displayName"), values=display_name))
    if display_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("displayName"), value=display_name_prefix))
    if description is not None and isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if timezone is not None and isinstance(timezone, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timezone"), value=timezone))
    if timezone and isinstance(timezone, list):
        filters.append(dm.filters.In(view_id.as_property_ref("timezone"), values=timezone))
    if timezone_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("timezone"), value=timezone_prefix))
    if default_method_day_ahead and isinstance(default_method_day_ahead, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("defaultMethodDayAhead"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": default_method_day_ahead},
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
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in default_method_day_ahead],
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
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
