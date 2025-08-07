from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

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
    IntFilter,
)
from cognite.powerops.client._generated.v1.data_classes._price_area import PriceArea, PriceAreaWrite


__all__ = [
    "PriceAreaAFRR",
    "PriceAreaAFRRWrite",
    "PriceAreaAFRRList",
    "PriceAreaAFRRWriteList",
    "PriceAreaAFRRFields",
    "PriceAreaAFRRTextFields",
    "PriceAreaAFRRGraphQL",
]


PriceAreaAFRRTextFields = Literal["external_id", "name", "display_name", "asset_type", "capacity_price_up", "capacity_price_down", "activation_price_up", "activation_price_down", "relative_activation", "total_capacity_allocation_up", "total_capacity_allocation_down", "own_capacity_allocation_up", "own_capacity_allocation_down"]
PriceAreaAFRRFields = Literal["external_id", "name", "display_name", "ordering", "asset_type", "capacity_price_up", "capacity_price_down", "activation_price_up", "activation_price_down", "relative_activation", "total_capacity_allocation_up", "total_capacity_allocation_down", "own_capacity_allocation_up", "own_capacity_allocation_down"]

_PRICEAREAAFRR_PROPERTIES_BY_FIELD = {
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
}


class PriceAreaAFRRGraphQL(GraphQLCore):
    """This represents the reading version of price area afrr, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area afrr.
        data_record: The data record of the price area afrr node.
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
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PriceAreaAFRR", "1")
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



    def as_read(self) -> PriceAreaAFRR:
        """Convert this GraphQL format of price area afrr to the reading format."""
        return PriceAreaAFRR.model_validate(as_read_args(self))

    def as_write(self) -> PriceAreaAFRRWrite:
        """Convert this GraphQL format of price area afrr to the writing format."""
        return PriceAreaAFRRWrite.model_validate(as_write_args(self))


class PriceAreaAFRR(PriceArea):
    """This represents the reading version of price area afrr.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area afrr.
        data_record: The data record of the price area afrr node.
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
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PriceAreaAFRR", "1")

    node_type: Union[dm.DirectRelationReference, None] = None
    capacity_price_up: Union[TimeSeries, str, None] = Field(None, alias="capacityPriceUp")
    capacity_price_down: Union[TimeSeries, str, None] = Field(None, alias="capacityPriceDown")
    activation_price_up: Union[TimeSeries, str, None] = Field(None, alias="activationPriceUp")
    activation_price_down: Union[TimeSeries, str, None] = Field(None, alias="activationPriceDown")
    relative_activation: Union[TimeSeries, str, None] = Field(None, alias="relativeActivation")
    total_capacity_allocation_up: Union[TimeSeries, str, None] = Field(None, alias="totalCapacityAllocationUp")
    total_capacity_allocation_down: Union[TimeSeries, str, None] = Field(None, alias="totalCapacityAllocationDown")
    own_capacity_allocation_up: Union[TimeSeries, str, None] = Field(None, alias="ownCapacityAllocationUp")
    own_capacity_allocation_down: Union[TimeSeries, str, None] = Field(None, alias="ownCapacityAllocationDown")


    def as_write(self) -> PriceAreaAFRRWrite:
        """Convert this read version of price area afrr to the writing version."""
        return PriceAreaAFRRWrite.model_validate(as_write_args(self))



class PriceAreaAFRRWrite(PriceAreaWrite):
    """This represents the writing version of price area afrr.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area afrr.
        data_record: The data record of the price area afrr node.
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
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("activation_price_down", "activation_price_up", "asset_type", "capacity_price_down", "capacity_price_up", "display_name", "name", "ordering", "own_capacity_allocation_down", "own_capacity_allocation_up", "relative_activation", "total_capacity_allocation_down", "total_capacity_allocation_up",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PriceAreaAFRR", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    capacity_price_up: Union[TimeSeriesWrite, str, None] = Field(None, alias="capacityPriceUp")
    capacity_price_down: Union[TimeSeriesWrite, str, None] = Field(None, alias="capacityPriceDown")
    activation_price_up: Union[TimeSeriesWrite, str, None] = Field(None, alias="activationPriceUp")
    activation_price_down: Union[TimeSeriesWrite, str, None] = Field(None, alias="activationPriceDown")
    relative_activation: Union[TimeSeriesWrite, str, None] = Field(None, alias="relativeActivation")
    total_capacity_allocation_up: Union[TimeSeriesWrite, str, None] = Field(None, alias="totalCapacityAllocationUp")
    total_capacity_allocation_down: Union[TimeSeriesWrite, str, None] = Field(None, alias="totalCapacityAllocationDown")
    own_capacity_allocation_up: Union[TimeSeriesWrite, str, None] = Field(None, alias="ownCapacityAllocationUp")
    own_capacity_allocation_down: Union[TimeSeriesWrite, str, None] = Field(None, alias="ownCapacityAllocationDown")



class PriceAreaAFRRList(DomainModelList[PriceAreaAFRR]):
    """List of price area afrrs in the read version."""

    _INSTANCE = PriceAreaAFRR
    def as_write(self) -> PriceAreaAFRRWriteList:
        """Convert these read versions of price area afrr to the writing versions."""
        return PriceAreaAFRRWriteList([node.as_write() for node in self.data])



class PriceAreaAFRRWriteList(DomainModelWriteList[PriceAreaAFRRWrite]):
    """List of price area afrrs in the writing version."""

    _INSTANCE = PriceAreaAFRRWrite


def _create_price_area_afrr_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    display_name: str | list[str] | None = None,
    display_name_prefix: str | None = None,
    min_ordering: int | None = None,
    max_ordering: int | None = None,
    asset_type: str | list[str] | None = None,
    asset_type_prefix: str | None = None,
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
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _PriceAreaAFRRQuery(NodeQueryCore[T_DomainModelList, PriceAreaAFRRList]):
    _view_id = PriceAreaAFRR._view_id
    _result_cls = PriceAreaAFRR
    _result_list_cls_end = PriceAreaAFRRList

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

    def list_price_area_afrr(self, limit: int = DEFAULT_QUERY_LIMIT) -> PriceAreaAFRRList:
        return self._list(limit=limit)


class PriceAreaAFRRQuery(_PriceAreaAFRRQuery[PriceAreaAFRRList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, PriceAreaAFRRList)
