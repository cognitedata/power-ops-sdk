from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import Field
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    TimeSeries,
)

if TYPE_CHECKING:
    from ._bid_method import BidMethod, BidMethodGraphQL, BidMethodWrite
    from ._plant import Plant, PlantGraphQL, PlantWrite
    from ._watercourse import Watercourse, WatercourseGraphQL, WatercourseWrite


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


class PriceAreaGraphQL(GraphQLCore):
    """This represents the reading version of price area, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

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

    view_id = dm.ViewId("power-ops-assets", "PriceArea", "1")
    name: Optional[str] = None
    display_name: Optional[str] = Field(None, alias="displayName")
    description: Optional[str] = None
    timezone: Optional[str] = None
    capacity_price_up: Union[TimeSeries, dict, None] = Field(None, alias="capacityPriceUp")
    capacity_price_down: Union[TimeSeries, dict, None] = Field(None, alias="capacityPriceDown")
    activation_price_up: Union[TimeSeries, dict, None] = Field(None, alias="activationPriceUp")
    activation_price_down: Union[TimeSeries, dict, None] = Field(None, alias="activationPriceDown")
    relative_activation: Union[TimeSeries, dict, None] = Field(None, alias="relativeActivation")
    total_capacity_allocation_up: Union[TimeSeries, dict, None] = Field(None, alias="totalCapacityAllocationUp")
    total_capacity_allocation_down: Union[TimeSeries, dict, None] = Field(None, alias="totalCapacityAllocationDown")
    own_capacity_allocation_up: Union[TimeSeries, dict, None] = Field(None, alias="ownCapacityAllocationUp")
    own_capacity_allocation_down: Union[TimeSeries, dict, None] = Field(None, alias="ownCapacityAllocationDown")
    default_method_day_ahead: Optional[BidMethodGraphQL] = Field(None, repr=False, alias="defaultMethodDayAhead")
    main_scenario_day_ahead: Union[TimeSeries, dict, None] = Field(None, alias="mainScenarioDayAhead")
    day_ahead_price: Union[TimeSeries, dict, None] = Field(None, alias="dayAheadPrice")
    plants: Optional[list[PlantGraphQL]] = Field(default=None, repr=False)
    watercourses: Optional[list[WatercourseGraphQL]] = Field(default=None, repr=False)

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

    @field_validator("default_method_day_ahead", "plants", "watercourses", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> PriceArea:
        """Convert this GraphQL format of price area to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return PriceArea(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
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
            default_method_day_ahead=(
                self.default_method_day_ahead.as_read()
                if isinstance(self.default_method_day_ahead, GraphQLCore)
                else self.default_method_day_ahead
            ),
            main_scenario_day_ahead=self.main_scenario_day_ahead,
            day_ahead_price=self.day_ahead_price,
            plants=[plant.as_read() if isinstance(plant, GraphQLCore) else plant for plant in self.plants or []],
            watercourses=[
                watercourse.as_read() if isinstance(watercourse, GraphQLCore) else watercourse
                for watercourse in self.watercourses or []
            ],
        )

    def as_write(self) -> PriceAreaWrite:
        """Convert this GraphQL format of price area to the writing format."""
        return PriceAreaWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
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
            default_method_day_ahead=(
                self.default_method_day_ahead.as_write()
                if isinstance(self.default_method_day_ahead, DomainModel)
                else self.default_method_day_ahead
            ),
            main_scenario_day_ahead=self.main_scenario_day_ahead,
            day_ahead_price=self.day_ahead_price,
            plants=[plant.as_write() if isinstance(plant, DomainModel) else plant for plant in self.plants or []],
            watercourses=[
                watercourse.as_write() if isinstance(watercourse, DomainModel) else watercourse
                for watercourse in self.watercourses or []
            ],
        )


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
    plants: Union[list[Plant], list[str], list[dm.NodeId], None] = Field(default=None, repr=False)
    watercourses: Union[list[Watercourse], list[str], list[dm.NodeId], None] = Field(default=None, repr=False)

    def as_write(self) -> PriceAreaWrite:
        """Convert this read version of price area to the writing version."""
        return PriceAreaWrite(
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
            default_method_day_ahead=(
                self.default_method_day_ahead.as_write()
                if isinstance(self.default_method_day_ahead, DomainModel)
                else self.default_method_day_ahead
            ),
            main_scenario_day_ahead=self.main_scenario_day_ahead,
            day_ahead_price=self.day_ahead_price,
            plants=[plant.as_write() if isinstance(plant, DomainModel) else plant for plant in self.plants or []],
            watercourses=[
                watercourse.as_write() if isinstance(watercourse, DomainModel) else watercourse
                for watercourse in self.watercourses or []
            ],
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
    default_method_day_ahead: Union[BidMethodWrite, str, dm.NodeId, None] = Field(
        None, repr=False, alias="defaultMethodDayAhead"
    )
    main_scenario_day_ahead: Union[TimeSeries, str, None] = Field(None, alias="mainScenarioDayAhead")
    day_ahead_price: Union[TimeSeries, str, None] = Field(None, alias="dayAheadPrice")
    plants: Union[list[PlantWrite], list[str], list[dm.NodeId], None] = Field(default=None, repr=False)
    watercourses: Union[list[WatercourseWrite], list[str], list[dm.NodeId], None] = Field(default=None, repr=False)

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
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
                "space": (
                    self.space
                    if isinstance(self.default_method_day_ahead, str)
                    else self.default_method_day_ahead.space
                ),
                "externalId": (
                    self.default_method_day_ahead
                    if isinstance(self.default_method_day_ahead, str)
                    else self.default_method_day_ahead.external_id
                ),
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
                existing_version=None if allow_version_increase else self.data_record.existing_version,
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
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=plant,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("power-ops-types", "isSubAssetOf")
        for watercourse in self.watercourses or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=watercourse,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.default_method_day_ahead, DomainModelWrite):
            other_resources = self.default_method_day_ahead._to_instances_write(cache, view_by_read_class)
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
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if isinstance(display_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("displayName"), value=display_name))
    if display_name and isinstance(display_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("displayName"), values=display_name))
    if display_name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("displayName"), value=display_name_prefix))
    if isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if isinstance(timezone, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timezone"), value=timezone))
    if timezone and isinstance(timezone, list):
        filters.append(dm.filters.In(view_id.as_property_ref("timezone"), values=timezone))
    if timezone_prefix is not None:
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
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
