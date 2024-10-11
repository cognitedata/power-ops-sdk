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
from ._price_area import PriceArea, PriceAreaWrite

if TYPE_CHECKING:
    from ._bid_configuration_day_ahead import BidConfigurationDayAhead, BidConfigurationDayAheadGraphQL, BidConfigurationDayAheadWrite


__all__ = [
    "PriceAreaDayAhead",
    "PriceAreaDayAheadWrite",
    "PriceAreaDayAheadApply",
    "PriceAreaDayAheadList",
    "PriceAreaDayAheadWriteList",
    "PriceAreaDayAheadApplyList",
    "PriceAreaDayAheadFields",
    "PriceAreaDayAheadTextFields",
    "PriceAreaDayAheadGraphQL",
]


PriceAreaDayAheadTextFields = Literal["name", "display_name", "asset_type", "main_price_scenario", "price_scenarios"]
PriceAreaDayAheadFields = Literal["name", "display_name", "ordering", "asset_type", "main_price_scenario", "price_scenarios"]

_PRICEAREADAYAHEAD_PROPERTIES_BY_FIELD = {
    "name": "name",
    "display_name": "displayName",
    "ordering": "ordering",
    "asset_type": "assetType",
    "main_price_scenario": "mainPriceScenario",
    "price_scenarios": "priceScenarios",
}

class PriceAreaDayAheadGraphQL(GraphQLCore):
    """This represents the reading version of price area day ahead, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area day ahead.
        data_record: The data record of the price area day ahead node.
        name: Name for the PowerAsset
        display_name: Display name for the PowerAsset.
        ordering: The ordering of the asset
        asset_type: The type of the asset
        default_bid_configuration: TODO
        main_price_scenario: TODO
        price_scenarios: TODO
    """
    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PriceAreaDayAhead", "1")
    name: Optional[str] = None
    display_name: Optional[str] = Field(None, alias="displayName")
    ordering: Optional[int] = None
    asset_type: Optional[str] = Field(None, alias="assetType")
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
    def as_read(self) -> PriceAreaDayAhead:
        """Convert this GraphQL format of price area day ahead to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return PriceAreaDayAhead(
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
            default_bid_configuration=self.default_bid_configuration.as_read() if isinstance(self.default_bid_configuration, GraphQLCore) else self.default_bid_configuration,
            main_price_scenario=self.main_price_scenario,
            price_scenarios=self.price_scenarios,
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> PriceAreaDayAheadWrite:
        """Convert this GraphQL format of price area day ahead to the writing format."""
        return PriceAreaDayAheadWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            display_name=self.display_name,
            ordering=self.ordering,
            asset_type=self.asset_type,
            default_bid_configuration=self.default_bid_configuration.as_write() if isinstance(self.default_bid_configuration, GraphQLCore) else self.default_bid_configuration,
            main_price_scenario=self.main_price_scenario,
            price_scenarios=self.price_scenarios,
        )


class PriceAreaDayAhead(PriceArea):
    """This represents the reading version of price area day ahead.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area day ahead.
        data_record: The data record of the price area day ahead node.
        name: Name for the PowerAsset
        display_name: Display name for the PowerAsset.
        ordering: The ordering of the asset
        asset_type: The type of the asset
        default_bid_configuration: TODO
        main_price_scenario: TODO
        price_scenarios: TODO
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PriceAreaDayAhead", "1")

    node_type: Union[dm.DirectRelationReference, None] = None
    default_bid_configuration: Union[BidConfigurationDayAhead, str, dm.NodeId, None] = Field(default=None, repr=False, alias="defaultBidConfiguration")
    main_price_scenario: Union[TimeSeries, str, None] = Field(None, alias="mainPriceScenario")
    price_scenarios: Union[list[TimeSeries], list[str], None] = Field(None, alias="priceScenarios")

    def as_write(self) -> PriceAreaDayAheadWrite:
        """Convert this read version of price area day ahead to the writing version."""
        return PriceAreaDayAheadWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            display_name=self.display_name,
            ordering=self.ordering,
            asset_type=self.asset_type,
            default_bid_configuration=self.default_bid_configuration.as_write() if isinstance(self.default_bid_configuration, DomainModel) else self.default_bid_configuration,
            main_price_scenario=self.main_price_scenario,
            price_scenarios=self.price_scenarios,
        )

    def as_apply(self) -> PriceAreaDayAheadWrite:
        """Convert this read version of price area day ahead to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PriceAreaDayAheadWrite(PriceAreaWrite):
    """This represents the writing version of price area day ahead.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area day ahead.
        data_record: The data record of the price area day ahead node.
        name: Name for the PowerAsset
        display_name: Display name for the PowerAsset.
        ordering: The ordering of the asset
        asset_type: The type of the asset
        default_bid_configuration: TODO
        main_price_scenario: TODO
        price_scenarios: TODO
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PriceAreaDayAhead", "1")

    node_type: Union[dm.DirectRelationReference, None] = None
    default_bid_configuration: Union[BidConfigurationDayAheadWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="defaultBidConfiguration")
    main_price_scenario: Union[TimeSeries, str, None] = Field(None, alias="mainPriceScenario")
    price_scenarios: Union[list[TimeSeries], list[str], None] = Field(None, alias="priceScenarios")

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

        if isinstance(self.main_price_scenario, CogniteTimeSeries):
            resources.time_series.append(self.main_price_scenario)

        if isinstance(self.price_scenarios, CogniteTimeSeries):
            resources.time_series.append(self.price_scenarios)

        return resources


class PriceAreaDayAheadApply(PriceAreaDayAheadWrite):
    def __new__(cls, *args, **kwargs) -> PriceAreaDayAheadApply:
        warnings.warn(
            "PriceAreaDayAheadApply is deprecated and will be removed in v1.0. Use PriceAreaDayAheadWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PriceAreaDayAhead.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class PriceAreaDayAheadList(DomainModelList[PriceAreaDayAhead]):
    """List of price area day aheads in the read version."""

    _INSTANCE = PriceAreaDayAhead

    def as_write(self) -> PriceAreaDayAheadWriteList:
        """Convert these read versions of price area day ahead to the writing versions."""
        return PriceAreaDayAheadWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PriceAreaDayAheadWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PriceAreaDayAheadWriteList(DomainModelWriteList[PriceAreaDayAheadWrite]):
    """List of price area day aheads in the writing version."""

    _INSTANCE = PriceAreaDayAheadWrite

class PriceAreaDayAheadApplyList(PriceAreaDayAheadWriteList): ...



def _create_price_area_day_ahead_filter(
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
