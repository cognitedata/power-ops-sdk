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

)


__all__ = [
    "ShopTimeResolution",
    "ShopTimeResolutionWrite",
    "ShopTimeResolutionList",
    "ShopTimeResolutionWriteList",
    "ShopTimeResolutionFields",
    "ShopTimeResolutionTextFields",
    "ShopTimeResolutionGraphQL",
]


ShopTimeResolutionTextFields = Literal["external_id", "name"]
ShopTimeResolutionFields = Literal["external_id", "name", "minutes_after_start", "time_resolution_minutes"]

_SHOPTIMERESOLUTION_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
    "minutes_after_start": "minutesAfterStart",
    "time_resolution_minutes": "timeResolutionMinutes",
}


class ShopTimeResolutionGraphQL(GraphQLCore):
    """This represents the reading version of shop time resolution, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop time resolution.
        data_record: The data record of the shop time resolution node.
        name: The name field.
        minutes_after_start: Minutes after SHOP Simulation start.
        time_resolution_minutes: The SHOP time resolution (in minutes) to use for SHOP.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopTimeResolution", "1")
    name: Optional[str] = None
    minutes_after_start: Optional[list[int]] = Field(None, alias="minutesAfterStart")
    time_resolution_minutes: Optional[list[int]] = Field(None, alias="timeResolutionMinutes")

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



    def as_read(self) -> ShopTimeResolution:
        """Convert this GraphQL format of shop time resolution to the reading format."""
        return ShopTimeResolution.model_validate(as_read_args(self))

    def as_write(self) -> ShopTimeResolutionWrite:
        """Convert this GraphQL format of shop time resolution to the writing format."""
        return ShopTimeResolutionWrite.model_validate(as_write_args(self))


class ShopTimeResolution(DomainModel):
    """This represents the reading version of shop time resolution.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop time resolution.
        data_record: The data record of the shop time resolution node.
        name: The name field.
        minutes_after_start: Minutes after SHOP Simulation start.
        time_resolution_minutes: The SHOP time resolution (in minutes) to use for SHOP.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopTimeResolution", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopTimeResolution")
    name: str
    minutes_after_start: list[int] = Field(alias="minutesAfterStart")
    time_resolution_minutes: list[int] = Field(alias="timeResolutionMinutes")


    def as_write(self) -> ShopTimeResolutionWrite:
        """Convert this read version of shop time resolution to the writing version."""
        return ShopTimeResolutionWrite.model_validate(as_write_args(self))



class ShopTimeResolutionWrite(DomainModelWrite):
    """This represents the writing version of shop time resolution.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop time resolution.
        data_record: The data record of the shop time resolution node.
        name: The name field.
        minutes_after_start: Minutes after SHOP Simulation start.
        time_resolution_minutes: The SHOP time resolution (in minutes) to use for SHOP.
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("minutes_after_start", "name", "time_resolution_minutes",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopTimeResolution", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "ShopTimeResolution")
    name: str
    minutes_after_start: list[int] = Field(alias="minutesAfterStart")
    time_resolution_minutes: list[int] = Field(alias="timeResolutionMinutes")



class ShopTimeResolutionList(DomainModelList[ShopTimeResolution]):
    """List of shop time resolutions in the read version."""

    _INSTANCE = ShopTimeResolution
    def as_write(self) -> ShopTimeResolutionWriteList:
        """Convert these read versions of shop time resolution to the writing versions."""
        return ShopTimeResolutionWriteList([node.as_write() for node in self.data])



class ShopTimeResolutionWriteList(DomainModelWriteList[ShopTimeResolutionWrite]):
    """List of shop time resolutions in the writing version."""

    _INSTANCE = ShopTimeResolutionWrite


def _create_shop_time_resolution_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
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
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _ShopTimeResolutionQuery(NodeQueryCore[T_DomainModelList, ShopTimeResolutionList]):
    _view_id = ShopTimeResolution._view_id
    _result_cls = ShopTimeResolution
    _result_list_cls_end = ShopTimeResolutionList

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
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
        ])

    def list_shop_time_resolution(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopTimeResolutionList:
        return self._list(limit=limit)


class ShopTimeResolutionQuery(_ShopTimeResolutionQuery[ShopTimeResolutionList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopTimeResolutionList)
