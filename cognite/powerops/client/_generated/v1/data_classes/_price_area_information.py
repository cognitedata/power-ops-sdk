from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    TimeSeries as CogniteTimeSeries,
    TimeSeriesWrite as CogniteTimeSeriesWrite,
)
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

from cognite.powerops.client._generated.v1.config import global_config
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
    as_node_id,
    as_read_args,
    as_write_args,
    is_tuple_id,
    as_instance_dict_id,
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    ViewPropertyId,
    DirectRelationFilter,
    IntFilter,
)
from cognite.powerops.client._generated.v1.data_classes._price_area_afrr import PriceAreaAFRR, PriceAreaAFRRWrite
from cognite.powerops.client._generated.v1.data_classes._price_area_day_ahead import PriceAreaDayAhead, PriceAreaDayAheadWrite
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._bid_configuration_day_ahead import BidConfigurationDayAhead, BidConfigurationDayAheadList, BidConfigurationDayAheadGraphQL, BidConfigurationDayAheadWrite, BidConfigurationDayAheadWriteList


__all__ = [
    "PriceAreaInformation",
    "PriceAreaInformationWrite",
    "PriceAreaInformationList",
    "PriceAreaInformationWriteList",
    "PriceAreaInformationFields",
    "PriceAreaInformationTextFields",
    "PriceAreaInformationGraphQL",
]


PriceAreaInformationTextFields = Literal["external_id", "name", "display_name", "asset_type", "capacity_price_up", "capacity_price_down", "activation_price_up", "activation_price_down", "relative_activation", "total_capacity_allocation_up", "total_capacity_allocation_down", "own_capacity_allocation_up", "own_capacity_allocation_down", "main_price_scenario", "price_scenarios"]
PriceAreaInformationFields = Literal["external_id", "name", "display_name", "ordering", "asset_type", "capacity_price_up", "capacity_price_down", "activation_price_up", "activation_price_down", "relative_activation", "total_capacity_allocation_up", "total_capacity_allocation_down", "own_capacity_allocation_up", "own_capacity_allocation_down", "main_price_scenario", "price_scenarios"]

_PRICEAREAINFORMATION_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
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
    capacity_price_up: Optional[TimeSeriesGraphQL] = Field(None, alias="capacityPriceUp")
    capacity_price_down: Optional[TimeSeriesGraphQL] = Field(None, alias="capacityPriceDown")
    activation_price_up: Optional[TimeSeriesGraphQL] = Field(None, alias="activationPriceUp")
    activation_price_down: Optional[TimeSeriesGraphQL] = Field(None, alias="activationPriceDown")
    relative_activation: Optional[TimeSeriesGraphQL] = Field(None, alias="relativeActivation")
    total_capacity_allocation_up: Optional[TimeSeriesGraphQL] = Field(None, alias="totalCapacityAllocationUp")
    total_capacity_allocation_down: Optional[TimeSeriesGraphQL] = Field(None, alias="totalCapacityAllocationDown")
    own_capacity_allocation_up: Optional[TimeSeriesGraphQL] = Field(None, alias="ownCapacityAllocationUp")
    own_capacity_allocation_down: Optional[TimeSeriesGraphQL] = Field(None, alias="ownCapacityAllocationDown")
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

    def as_read(self) -> PriceAreaInformation:
        """Convert this GraphQL format of price area information to the reading format."""
        return PriceAreaInformation.model_validate(as_read_args(self))

    def as_write(self) -> PriceAreaInformationWrite:
        """Convert this GraphQL format of price area information to the writing format."""
        return PriceAreaInformationWrite.model_validate(as_write_args(self))


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
    @field_validator("default_bid_configuration", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)


    def as_write(self) -> PriceAreaInformationWrite:
        """Convert this read version of price area information to the writing version."""
        return PriceAreaInformationWrite.model_validate(as_write_args(self))



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
    _container_fields: ClassVar[tuple[str, ...]] = ("activation_price_down", "activation_price_up", "asset_type", "capacity_price_down", "capacity_price_up", "default_bid_configuration", "display_name", "main_price_scenario", "name", "ordering", "own_capacity_allocation_down", "own_capacity_allocation_up", "price_scenarios", "relative_activation", "total_capacity_allocation_down", "total_capacity_allocation_up",)
    _direct_relations: ClassVar[tuple[str, ...]] = ("default_bid_configuration",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PriceAreaInformation", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None



class PriceAreaInformationList(DomainModelList[PriceAreaInformation]):
    """List of price area information in the read version."""

    _INSTANCE = PriceAreaInformation
    def as_write(self) -> PriceAreaInformationWriteList:
        """Convert these read versions of price area information to the writing versions."""
        return PriceAreaInformationWriteList([node.as_write() for node in self.data])


    @property
    def default_bid_configuration(self) -> BidConfigurationDayAheadList:
        from ._bid_configuration_day_ahead import BidConfigurationDayAhead, BidConfigurationDayAheadList
        return BidConfigurationDayAheadList([item.default_bid_configuration for item in self.data if isinstance(item.default_bid_configuration, BidConfigurationDayAhead)])

class PriceAreaInformationWriteList(DomainModelWriteList[PriceAreaInformationWrite]):
    """List of price area information in the writing version."""

    _INSTANCE = PriceAreaInformationWrite
    @property
    def default_bid_configuration(self) -> BidConfigurationDayAheadWriteList:
        from ._bid_configuration_day_ahead import BidConfigurationDayAheadWrite, BidConfigurationDayAheadWriteList
        return BidConfigurationDayAheadWriteList([item.default_bid_configuration for item in self.data if isinstance(item.default_bid_configuration, BidConfigurationDayAheadWrite)])


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


class _PriceAreaInformationQuery(NodeQueryCore[T_DomainModelList, PriceAreaInformationList]):
    _view_id = PriceAreaInformation._view_id
    _result_cls = PriceAreaInformation
    _result_list_cls_end = PriceAreaInformationList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
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
            connection_property,
            connection_type,
            reverse_expression,
        )

        if _BidConfigurationDayAheadQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "defaultBidConfiguration"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.display_name = StringFilter(self, self._view_id.as_property_ref("displayName"))
        self.ordering = IntFilter(self, self._view_id.as_property_ref("ordering"))
        self.asset_type = StringFilter(self, self._view_id.as_property_ref("assetType"))
        self.default_bid_configuration_filter = DirectRelationFilter(self, self._view_id.as_property_ref("defaultBidConfiguration"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.display_name,
            self.ordering,
            self.asset_type,
            self.default_bid_configuration_filter,
        ])
        self.capacity_price_up = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.capacity_price_up if isinstance(item.capacity_price_up, str) else item.capacity_price_up.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.capacity_price_up is not None and
               (isinstance(item.capacity_price_up, str) or item.capacity_price_up.external_id is not None)
        ])
        self.capacity_price_down = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.capacity_price_down if isinstance(item.capacity_price_down, str) else item.capacity_price_down.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.capacity_price_down is not None and
               (isinstance(item.capacity_price_down, str) or item.capacity_price_down.external_id is not None)
        ])
        self.activation_price_up = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.activation_price_up if isinstance(item.activation_price_up, str) else item.activation_price_up.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.activation_price_up is not None and
               (isinstance(item.activation_price_up, str) or item.activation_price_up.external_id is not None)
        ])
        self.activation_price_down = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.activation_price_down if isinstance(item.activation_price_down, str) else item.activation_price_down.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.activation_price_down is not None and
               (isinstance(item.activation_price_down, str) or item.activation_price_down.external_id is not None)
        ])
        self.relative_activation = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.relative_activation if isinstance(item.relative_activation, str) else item.relative_activation.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.relative_activation is not None and
               (isinstance(item.relative_activation, str) or item.relative_activation.external_id is not None)
        ])
        self.total_capacity_allocation_up = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.total_capacity_allocation_up if isinstance(item.total_capacity_allocation_up, str) else item.total_capacity_allocation_up.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.total_capacity_allocation_up is not None and
               (isinstance(item.total_capacity_allocation_up, str) or item.total_capacity_allocation_up.external_id is not None)
        ])
        self.total_capacity_allocation_down = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.total_capacity_allocation_down if isinstance(item.total_capacity_allocation_down, str) else item.total_capacity_allocation_down.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.total_capacity_allocation_down is not None and
               (isinstance(item.total_capacity_allocation_down, str) or item.total_capacity_allocation_down.external_id is not None)
        ])
        self.own_capacity_allocation_up = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.own_capacity_allocation_up if isinstance(item.own_capacity_allocation_up, str) else item.own_capacity_allocation_up.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.own_capacity_allocation_up is not None and
               (isinstance(item.own_capacity_allocation_up, str) or item.own_capacity_allocation_up.external_id is not None)
        ])
        self.own_capacity_allocation_down = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.own_capacity_allocation_down if isinstance(item.own_capacity_allocation_down, str) else item.own_capacity_allocation_down.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.own_capacity_allocation_down is not None and
               (isinstance(item.own_capacity_allocation_down, str) or item.own_capacity_allocation_down.external_id is not None)
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

    def list_price_area_information(self, limit: int = DEFAULT_QUERY_LIMIT) -> PriceAreaInformationList:
        return self._list(limit=limit)


class PriceAreaInformationQuery(_PriceAreaInformationQuery[PriceAreaInformationList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, PriceAreaInformationList)
