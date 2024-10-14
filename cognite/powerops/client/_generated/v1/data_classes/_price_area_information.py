from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Literal,  no_type_check, Optional, Union

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
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    TimeSeries,
)
from ._price_area_afrr import PriceAreaAFRR, PriceAreaAFRRWrite
from ._price_area_day_ahead import PriceAreaDayAhead, PriceAreaDayAheadWrite

if TYPE_CHECKING:
    from ._bid_configuration_day_ahead import BidConfigurationDayAhead, BidConfigurationDayAheadGraphQL, BidConfigurationDayAheadWrite


__all__ = [
    "PriceAreaInformation",
    "PriceAreaInformationWrite",
    "PriceAreaInformationApply",
    "PriceAreaInformationList",
    "PriceAreaInformationWriteList",
    "PriceAreaInformationApplyList",
    "PriceAreaInformationFields",
    "PriceAreaInformationTextFields",
    "PriceAreaInformationGraphQL",
]


PriceAreaInformationTextFields = Literal["name", "display_name", "asset_type", "capacity_price_up", "capacity_price_down", "activation_price_up", "activation_price_down", "relative_activation", "total_capacity_allocation_up", "total_capacity_allocation_down", "own_capacity_allocation_up", "own_capacity_allocation_down", "main_price_scenario", "price_scenarios"]
PriceAreaInformationFields = Literal["name", "display_name", "ordering", "asset_type", "capacity_price_up", "capacity_price_down", "activation_price_up", "activation_price_down", "relative_activation", "total_capacity_allocation_up", "total_capacity_allocation_down", "own_capacity_allocation_up", "own_capacity_allocation_down", "main_price_scenario", "price_scenarios"]

_PRICEAREAINFORMATION_PROPERTIES_BY_FIELD = {
    "name": "name",
    "display_name": "displayName",
    "ordering": "ordering",
    "asset_type": "assetType",
    "capacity_price_up": "capacityPriceUp",
    "capacity_price_down": "capacityPriceDown",
    "activation_price_up": "activationPriceUp",
    "activation_price_down": "activationPriceDown",
    "relative_activation": "relativeActivation",
    "total_capacity_allocation_up": "totalCapacityAllocationUp",
    "total_capacity_allocation_down": "totalCapacityAllocationDown",
    "own_capacity_allocation_up": "ownCapacityAllocationUp",
    "own_capacity_allocation_down": "ownCapacityAllocationDown",
    "main_price_scenario": "mainPriceScenario",
    "price_scenarios": "priceScenarios",
}

class PriceAreaInformationGraphQL(GraphQLCore):
    """This represents the reading version of price area information, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area information.
        data_record: The data record of the price area information node.
        name: Name for the PowerAsset
        display_name: Display name for the PowerAsset.
        ordering: The ordering of the asset
        asset_type: The type of the asset
        capacity_price_up: The capacity price up field.
        capacity_price_down: The capacity price down field.
        activation_price_up: The mFRR activation price (TBC)
        activation_price_down: The mFRR activate price (TBC)
        relative_activation: Value between -1 (100 % activation down) and 1 (100 % activation down)
        total_capacity_allocation_up: The total capacity allocation up field.
        total_capacity_allocation_down: The total capacity allocation down field.
        own_capacity_allocation_up: The own capacity allocation up field.
        own_capacity_allocation_down: The own capacity allocation down field.
        default_bid_configuration: TODO
        main_price_scenario: TODO
        price_scenarios: TODO
    """
    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PriceAreaInformation", "1")
    name: Optional[str] = None
    display_name: Optional[str] = Field(None, alias="displayName")
    ordering: Optional[int] = None
    asset_type: Optional[str] = Field(None, alias="assetType")
    capacity_price_up: Union[TimeSeries, dict, None] = Field(None, alias="capacityPriceUp")
    capacity_price_down: Union[TimeSeries, dict, None] = Field(None, alias="capacityPriceDown")
    activation_price_up: Union[TimeSeries, dict, None] = Field(None, alias="activationPriceUp")
    activation_price_down: Union[TimeSeries, dict, None] = Field(None, alias="activationPriceDown")
    relative_activation: Union[TimeSeries, dict, None] = Field(None, alias="relativeActivation")
    total_capacity_allocation_up: Union[TimeSeries, dict, None] = Field(None, alias="totalCapacityAllocationUp")
    total_capacity_allocation_down: Union[TimeSeries, dict, None] = Field(None, alias="totalCapacityAllocationDown")
    own_capacity_allocation_up: Union[TimeSeries, dict, None] = Field(None, alias="ownCapacityAllocationUp")
    own_capacity_allocation_down: Union[TimeSeries, dict, None] = Field(None, alias="ownCapacityAllocationDown")
    default_bid_configuration: Optional[BidConfigurationDayAheadGraphQL] = Field(default=None, repr=False, alias="defaultBidConfiguration")
    main_price_scenario: Union[TimeSeries, dict, None] = Field(None, alias="mainPriceScenario")
    price_scenarios: Union[list[TimeSeries], list[dict], None] = Field(None, alias="priceScenarios")

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
    @field_validator("price_scenarios", mode="before")
    def clean_list(cls, value: Any) -> Any:
        if isinstance(value, list):
            return [v for v in value if v is not None] or None
        return value

    @field_validator("default_bid_configuration", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> PriceAreaInformation:
        """Convert this GraphQL format of price area information to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return PriceAreaInformation(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            display_name=self.display_name,
            ordering=self.ordering,
            asset_type=self.asset_type,
            capacity_price_up=self.capacity_price_up,
            capacity_price_down=self.capacity_price_down,
            activation_price_up=self.activation_price_up,
            activation_price_down=self.activation_price_down,
            relative_activation=self.relative_activation,
            total_capacity_allocation_up=self.total_capacity_allocation_up,
            total_capacity_allocation_down=self.total_capacity_allocation_down,
            own_capacity_allocation_up=self.own_capacity_allocation_up,
            own_capacity_allocation_down=self.own_capacity_allocation_down,
            default_bid_configuration=self.default_bid_configuration.as_read() if isinstance(self.default_bid_configuration, GraphQLCore) else self.default_bid_configuration,
            main_price_scenario=self.main_price_scenario,
            price_scenarios=self.price_scenarios,
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> PriceAreaInformationWrite:
        """Convert this GraphQL format of price area information to the writing format."""
        return PriceAreaInformationWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            display_name=self.display_name,
            ordering=self.ordering,
            asset_type=self.asset_type,
            capacity_price_up=self.capacity_price_up,
            capacity_price_down=self.capacity_price_down,
            activation_price_up=self.activation_price_up,
            activation_price_down=self.activation_price_down,
            relative_activation=self.relative_activation,
            total_capacity_allocation_up=self.total_capacity_allocation_up,
            total_capacity_allocation_down=self.total_capacity_allocation_down,
            own_capacity_allocation_up=self.own_capacity_allocation_up,
            own_capacity_allocation_down=self.own_capacity_allocation_down,
            default_bid_configuration=self.default_bid_configuration.as_write() if isinstance(self.default_bid_configuration, GraphQLCore) else self.default_bid_configuration,
            main_price_scenario=self.main_price_scenario,
            price_scenarios=self.price_scenarios,
        )


class PriceAreaInformation(PriceAreaAFRR, PriceAreaDayAhead):
    """This represents the reading version of price area information.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area information.
        data_record: The data record of the price area information node.
        name: Name for the PowerAsset
        display_name: Display name for the PowerAsset.
        ordering: The ordering of the asset
        asset_type: The type of the asset
        capacity_price_up: The capacity price up field.
        capacity_price_down: The capacity price down field.
        activation_price_up: The mFRR activation price (TBC)
        activation_price_down: The mFRR activate price (TBC)
        relative_activation: Value between -1 (100 % activation down) and 1 (100 % activation down)
        total_capacity_allocation_up: The total capacity allocation up field.
        total_capacity_allocation_down: The total capacity allocation down field.
        own_capacity_allocation_up: The own capacity allocation up field.
        own_capacity_allocation_down: The own capacity allocation down field.
        default_bid_configuration: TODO
        main_price_scenario: TODO
        price_scenarios: TODO
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PriceAreaInformation", "1")

    node_type: Union[dm.DirectRelationReference, None] = None

    def as_write(self) -> PriceAreaInformationWrite:
        """Convert this read version of price area information to the writing version."""
        return PriceAreaInformationWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            display_name=self.display_name,
            ordering=self.ordering,
            asset_type=self.asset_type,
            capacity_price_up=self.capacity_price_up,
            capacity_price_down=self.capacity_price_down,
            activation_price_up=self.activation_price_up,
            activation_price_down=self.activation_price_down,
            relative_activation=self.relative_activation,
            total_capacity_allocation_up=self.total_capacity_allocation_up,
            total_capacity_allocation_down=self.total_capacity_allocation_down,
            own_capacity_allocation_up=self.own_capacity_allocation_up,
            own_capacity_allocation_down=self.own_capacity_allocation_down,
            default_bid_configuration=self.default_bid_configuration.as_write() if isinstance(self.default_bid_configuration, DomainModel) else self.default_bid_configuration,
            main_price_scenario=self.main_price_scenario,
            price_scenarios=self.price_scenarios,
        )

    def as_apply(self) -> PriceAreaInformationWrite:
        """Convert this read version of price area information to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PriceAreaInformationWrite(PriceAreaAFRRWrite, PriceAreaDayAheadWrite):
    """This represents the writing version of price area information.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area information.
        data_record: The data record of the price area information node.
        name: Name for the PowerAsset
        display_name: Display name for the PowerAsset.
        ordering: The ordering of the asset
        asset_type: The type of the asset
        capacity_price_up: The capacity price up field.
        capacity_price_down: The capacity price down field.
        activation_price_up: The mFRR activation price (TBC)
        activation_price_down: The mFRR activate price (TBC)
        relative_activation: Value between -1 (100 % activation down) and 1 (100 % activation down)
        total_capacity_allocation_up: The total capacity allocation up field.
        total_capacity_allocation_down: The total capacity allocation down field.
        own_capacity_allocation_up: The own capacity allocation up field.
        own_capacity_allocation_down: The own capacity allocation down field.
        default_bid_configuration: TODO
        main_price_scenario: TODO
        price_scenarios: TODO
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PriceAreaInformation", "1")

    node_type: Union[dm.DirectRelationReference, None] = None

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

        if self.display_name is not None or write_none:
            properties["displayName"] = self.display_name

        if self.ordering is not None or write_none:
            properties["ordering"] = self.ordering

        if self.asset_type is not None or write_none:
            properties["assetType"] = self.asset_type

        if self.capacity_price_up is not None or write_none:
            properties["capacityPriceUp"] = self.capacity_price_up if isinstance(self.capacity_price_up, str) or self.capacity_price_up is None else self.capacity_price_up.external_id

        if self.capacity_price_down is not None or write_none:
            properties["capacityPriceDown"] = self.capacity_price_down if isinstance(self.capacity_price_down, str) or self.capacity_price_down is None else self.capacity_price_down.external_id

        if self.activation_price_up is not None or write_none:
            properties["activationPriceUp"] = self.activation_price_up if isinstance(self.activation_price_up, str) or self.activation_price_up is None else self.activation_price_up.external_id

        if self.activation_price_down is not None or write_none:
            properties["activationPriceDown"] = self.activation_price_down if isinstance(self.activation_price_down, str) or self.activation_price_down is None else self.activation_price_down.external_id

        if self.relative_activation is not None or write_none:
            properties["relativeActivation"] = self.relative_activation if isinstance(self.relative_activation, str) or self.relative_activation is None else self.relative_activation.external_id

        if self.total_capacity_allocation_up is not None or write_none:
            properties["totalCapacityAllocationUp"] = self.total_capacity_allocation_up if isinstance(self.total_capacity_allocation_up, str) or self.total_capacity_allocation_up is None else self.total_capacity_allocation_up.external_id

        if self.total_capacity_allocation_down is not None or write_none:
            properties["totalCapacityAllocationDown"] = self.total_capacity_allocation_down if isinstance(self.total_capacity_allocation_down, str) or self.total_capacity_allocation_down is None else self.total_capacity_allocation_down.external_id

        if self.own_capacity_allocation_up is not None or write_none:
            properties["ownCapacityAllocationUp"] = self.own_capacity_allocation_up if isinstance(self.own_capacity_allocation_up, str) or self.own_capacity_allocation_up is None else self.own_capacity_allocation_up.external_id

        if self.own_capacity_allocation_down is not None or write_none:
            properties["ownCapacityAllocationDown"] = self.own_capacity_allocation_down if isinstance(self.own_capacity_allocation_down, str) or self.own_capacity_allocation_down is None else self.own_capacity_allocation_down.external_id

        if self.default_bid_configuration is not None:
            properties["defaultBidConfiguration"] = {
                "space":  self.space if isinstance(self.default_bid_configuration, str) else self.default_bid_configuration.space,
                "externalId": self.default_bid_configuration if isinstance(self.default_bid_configuration, str) else self.default_bid_configuration.external_id,
            }

        if self.main_price_scenario is not None or write_none:
            properties["mainPriceScenario"] = self.main_price_scenario if isinstance(self.main_price_scenario, str) or self.main_price_scenario is None else self.main_price_scenario.external_id

        if self.price_scenarios is not None or write_none:
            properties["priceScenarios"] = [value if isinstance(value, str) else value.external_id for value in self.price_scenarios or []] or None


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



        if isinstance(self.default_bid_configuration, DomainModelWrite):
            other_resources = self.default_bid_configuration._to_instances_write(cache)
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

        if isinstance(self.main_price_scenario, CogniteTimeSeries):
            resources.time_series.append(self.main_price_scenario)

        if isinstance(self.price_scenarios, CogniteTimeSeries):
            resources.time_series.append(self.price_scenarios)

        return resources


class PriceAreaInformationApply(PriceAreaInformationWrite):
    def __new__(cls, *args, **kwargs) -> PriceAreaInformationApply:
        warnings.warn(
            "PriceAreaInformationApply is deprecated and will be removed in v1.0. Use PriceAreaInformationWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PriceAreaInformation.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class PriceAreaInformationList(DomainModelList[PriceAreaInformation]):
    """List of price area information in the read version."""

    _INSTANCE = PriceAreaInformation

    def as_write(self) -> PriceAreaInformationWriteList:
        """Convert these read versions of price area information to the writing versions."""
        return PriceAreaInformationWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PriceAreaInformationWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PriceAreaInformationWriteList(DomainModelWriteList[PriceAreaInformationWrite]):
    """List of price area information in the writing version."""

    _INSTANCE = PriceAreaInformationWrite

class PriceAreaInformationApplyList(PriceAreaInformationWriteList): ...



def _create_price_area_information_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    display_name: str | list[str] | None = None,
    display_name_prefix: str | None = None,
    min_ordering: int | None = None,
    max_ordering: int | None = None,
    asset_type: str | list[str] | None = None,
    asset_type_prefix: str | None = None,
    default_bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if isinstance(display_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("displayName"), value=display_name))
    if display_name and isinstance(display_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("displayName"), values=display_name))
    if display_name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("displayName"), value=display_name_prefix))
    if min_ordering is not None or max_ordering is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("ordering"), gte=min_ordering, lte=max_ordering))
    if isinstance(asset_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("assetType"), value=asset_type))
    if asset_type and isinstance(asset_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("assetType"), values=asset_type))
    if asset_type_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("assetType"), value=asset_type_prefix))
    if default_bid_configuration and isinstance(default_bid_configuration, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("defaultBidConfiguration"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": default_bid_configuration}))
    if default_bid_configuration and isinstance(default_bid_configuration, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("defaultBidConfiguration"), value={"space": default_bid_configuration[0], "externalId": default_bid_configuration[1]}))
    if default_bid_configuration and isinstance(default_bid_configuration, list) and isinstance(default_bid_configuration[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("defaultBidConfiguration"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in default_bid_configuration]))
    if default_bid_configuration and isinstance(default_bid_configuration, list) and isinstance(default_bid_configuration[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("defaultBidConfiguration"), values=[{"space": item[0], "externalId": item[1]} for item in default_bid_configuration]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
