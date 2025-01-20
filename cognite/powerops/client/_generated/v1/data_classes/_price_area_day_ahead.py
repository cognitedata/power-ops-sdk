from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal,  no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    TimeSeries as CogniteTimeSeries,
    TimeSeriesWrite as CogniteTimeSeriesWrite,
)
from pydantic import Field
from pydantic import field_validator, model_validator

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
    FileMetadata,
    FileMetadataWrite,
    FileMetadataGraphQL,
    TimeSeries,
    TimeSeriesWrite,
    TimeSeriesGraphQL,
    TimeSeriesReferenceAPI,
    T_DomainModelList,
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    is_tuple_id,
    select_best_node,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    IntFilter,
)
from cognite.powerops.client._generated.v1.data_classes._price_area import PriceArea, PriceAreaWrite
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._bid_configuration_day_ahead import BidConfigurationDayAhead, BidConfigurationDayAheadList, BidConfigurationDayAheadGraphQL, BidConfigurationDayAheadWrite, BidConfigurationDayAheadWriteList


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


PriceAreaDayAheadTextFields = Literal["external_id", "name", "display_name", "asset_type", "main_price_scenario", "price_scenarios"]
PriceAreaDayAheadFields = Literal["external_id", "name", "display_name", "ordering", "asset_type", "main_price_scenario", "price_scenarios"]

_PRICEAREADAYAHEAD_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
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
    main_price_scenario: Optional[TimeSeriesGraphQL] = Field(None, alias="mainPriceScenario")
    price_scenarios: Optional[list[TimeSeriesGraphQL]] = Field(None, alias="priceScenarios")

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
            space=self.space,
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
            default_bid_configuration=self.default_bid_configuration.as_read()
if isinstance(self.default_bid_configuration, GraphQLCore)
else self.default_bid_configuration,
            main_price_scenario=self.main_price_scenario.as_read() if self.main_price_scenario else None,
            price_scenarios=[price_scenario.as_read() for price_scenario in self.price_scenarios or []] if self.price_scenarios is not None else None,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> PriceAreaDayAheadWrite:
        """Convert this GraphQL format of price area day ahead to the writing format."""
        return PriceAreaDayAheadWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            display_name=self.display_name,
            ordering=self.ordering,
            asset_type=self.asset_type,
            default_bid_configuration=self.default_bid_configuration.as_write()
if isinstance(self.default_bid_configuration, GraphQLCore)
else self.default_bid_configuration,
            main_price_scenario=self.main_price_scenario.as_write() if self.main_price_scenario else None,
            price_scenarios=[price_scenario.as_write() for price_scenario in self.price_scenarios or []] if self.price_scenarios is not None else None,
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
    price_scenarios: Optional[list[Union[TimeSeries, str]]] = Field(None, alias="priceScenarios")

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
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
            default_bid_configuration=self.default_bid_configuration.as_write()
if isinstance(self.default_bid_configuration, DomainModel)
else self.default_bid_configuration,
            main_price_scenario=self.main_price_scenario.as_write() if isinstance(self.main_price_scenario, CogniteTimeSeries) else self.main_price_scenario,
            price_scenarios=[price_scenario.as_write() if isinstance(price_scenario, CogniteTimeSeries) else price_scenario for price_scenario in self.price_scenarios] if self.price_scenarios is not None else None,
        )

    def as_apply(self) -> PriceAreaDayAheadWrite:
        """Convert this read version of price area day ahead to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()
    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, PriceAreaDayAhead],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._bid_configuration_day_ahead import BidConfigurationDayAhead
        for instance in instances.values():
            if isinstance(instance.default_bid_configuration, (dm.NodeId, str)) and (default_bid_configuration := nodes_by_id.get(instance.default_bid_configuration)) and isinstance(
                    default_bid_configuration, BidConfigurationDayAhead
            ):
                instance.default_bid_configuration = default_bid_configuration


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

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    default_bid_configuration: Union[BidConfigurationDayAheadWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="defaultBidConfiguration")
    main_price_scenario: Union[TimeSeriesWrite, str, None] = Field(None, alias="mainPriceScenario")
    price_scenarios: Optional[list[Union[TimeSeriesWrite, str]]] = Field(None, alias="priceScenarios")

    @field_validator("default_bid_configuration", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value

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
            properties["priceScenarios"] = [price_scenario if isinstance(price_scenario, str) else price_scenario.external_id for price_scenario in self.price_scenarios or []] if self.price_scenarios is not None else None

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=as_direct_relation_reference(self.node_type),
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

        if isinstance(self.main_price_scenario, CogniteTimeSeriesWrite):
            resources.time_series.append(self.main_price_scenario)

        for price_scenario in self.price_scenarios or []:
            if isinstance(price_scenario, CogniteTimeSeriesWrite):
                resources.time_series.append(price_scenario)

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

    @property
    def default_bid_configuration(self) -> BidConfigurationDayAheadList:
        from ._bid_configuration_day_ahead import BidConfigurationDayAhead, BidConfigurationDayAheadList
        return BidConfigurationDayAheadList([item.default_bid_configuration for item in self.data if isinstance(item.default_bid_configuration, BidConfigurationDayAhead)])

class PriceAreaDayAheadWriteList(DomainModelWriteList[PriceAreaDayAheadWrite]):
    """List of price area day aheads in the writing version."""

    _INSTANCE = PriceAreaDayAheadWrite
    @property
    def default_bid_configuration(self) -> BidConfigurationDayAheadWriteList:
        from ._bid_configuration_day_ahead import BidConfigurationDayAheadWrite, BidConfigurationDayAheadWriteList
        return BidConfigurationDayAheadWriteList([item.default_bid_configuration for item in self.data if isinstance(item.default_bid_configuration, BidConfigurationDayAheadWrite)])

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
    default_bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
    if isinstance(default_bid_configuration, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(default_bid_configuration):
        filters.append(dm.filters.Equals(view_id.as_property_ref("defaultBidConfiguration"), value=as_instance_dict_id(default_bid_configuration)))
    if default_bid_configuration and isinstance(default_bid_configuration, Sequence) and not isinstance(default_bid_configuration, str) and not is_tuple_id(default_bid_configuration):
        filters.append(dm.filters.In(view_id.as_property_ref("defaultBidConfiguration"), values=[as_instance_dict_id(item) for item in default_bid_configuration]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _PriceAreaDayAheadQuery(NodeQueryCore[T_DomainModelList, PriceAreaDayAheadList]):
    _view_id = PriceAreaDayAhead._view_id
    _result_cls = PriceAreaDayAhead
    _result_list_cls_end = PriceAreaDayAheadList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
    ):
        from ._bid_configuration_day_ahead import _BidConfigurationDayAheadQuery

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
            connection_type,
            reverse_expression,
        )

        if _BidConfigurationDayAheadQuery not in created_types:
            self.default_bid_configuration = _BidConfigurationDayAheadQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("defaultBidConfiguration"),
                    direction="outwards",
                ),
                connection_name="default_bid_configuration",
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.display_name = StringFilter(self, self._view_id.as_property_ref("displayName"))
        self.ordering = IntFilter(self, self._view_id.as_property_ref("ordering"))
        self.asset_type = StringFilter(self, self._view_id.as_property_ref("assetType"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.display_name,
            self.ordering,
            self.asset_type,
        ])
        self.main_price_scenario = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.main_price_scenario if isinstance(item.main_price_scenario, str) else item.main_price_scenario.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.main_price_scenario is not None and
               (isinstance(item.main_price_scenario, str) or item.main_price_scenario.external_id is not None)
        ])
        self.price_scenarios = TimeSeriesReferenceAPI(client,  lambda limit: [
            ts if isinstance(ts, str) else ts.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.price_scenarios is not None
            for ts in item.price_scenarios
            if ts is not None and
               (isinstance(ts, str) or ts.external_id is not None)
        ])

    def list_price_area_day_ahead(self, limit: int = DEFAULT_QUERY_LIMIT) -> PriceAreaDayAheadList:
        return self._list(limit=limit)


class PriceAreaDayAheadQuery(_PriceAreaDayAheadQuery[PriceAreaDayAheadList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, PriceAreaDayAheadList)
