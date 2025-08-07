from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
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
from cognite.powerops.client._generated.v1.data_classes._power_asset import PowerAsset, PowerAssetWrite


__all__ = [
    "Plant",
    "PlantWrite",
    "PlantList",
    "PlantWriteList",
    "PlantFields",
    "PlantTextFields",
    "PlantGraphQL",
]


PlantTextFields = Literal["external_id", "name", "display_name", "asset_type"]
PlantFields = Literal["external_id", "name", "display_name", "ordering", "asset_type"]

_PLANT_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
    "display_name": "displayName",
    "ordering": "ordering",
    "asset_type": "assetType",
}


class PlantGraphQL(GraphQLCore):
    """This represents the reading version of plant, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the plant.
        data_record: The data record of the plant node.
        name: Name for the PowerAsset
        display_name: Display name for the PowerAsset.
        ordering: The ordering of the asset
        asset_type: The type of the asset
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "Plant", "1")
    name: Optional[str] = None
    display_name: Optional[str] = Field(None, alias="displayName")
    ordering: Optional[int] = None
    asset_type: Optional[str] = Field(None, alias="assetType")

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



    def as_read(self) -> Plant:
        """Convert this GraphQL format of plant to the reading format."""
        return Plant.model_validate(as_read_args(self))

    def as_write(self) -> PlantWrite:
        """Convert this GraphQL format of plant to the writing format."""
        return PlantWrite.model_validate(as_write_args(self))


class Plant(PowerAsset):
    """This represents the reading version of plant.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the plant.
        data_record: The data record of the plant node.
        name: Name for the PowerAsset
        display_name: Display name for the PowerAsset.
        ordering: The ordering of the asset
        asset_type: The type of the asset
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "Plant", "1")

    node_type: Union[dm.DirectRelationReference, None] = None


    def as_write(self) -> PlantWrite:
        """Convert this read version of plant to the writing version."""
        return PlantWrite.model_validate(as_write_args(self))



class PlantWrite(PowerAssetWrite):
    """This represents the writing version of plant.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the plant.
        data_record: The data record of the plant node.
        name: Name for the PowerAsset
        display_name: Display name for the PowerAsset.
        ordering: The ordering of the asset
        asset_type: The type of the asset
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("asset_type", "display_name", "name", "ordering",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "Plant", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None



class PlantList(DomainModelList[Plant]):
    """List of plants in the read version."""

    _INSTANCE = Plant
    def as_write(self) -> PlantWriteList:
        """Convert these read versions of plant to the writing versions."""
        return PlantWriteList([node.as_write() for node in self.data])



class PlantWriteList(DomainModelWriteList[PlantWrite]):
    """List of plants in the writing version."""

    _INSTANCE = PlantWrite


def _create_plant_filter(
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


class _PlantQuery(NodeQueryCore[T_DomainModelList, PlantList]):
    _view_id = Plant._view_id
    _result_cls = Plant
    _result_list_cls_end = PlantList

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

    def list_plant(self, limit: int = DEFAULT_QUERY_LIMIT) -> PlantList:
        return self._list(limit=limit)


class PlantQuery(_PlantQuery[PlantList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, PlantList)
