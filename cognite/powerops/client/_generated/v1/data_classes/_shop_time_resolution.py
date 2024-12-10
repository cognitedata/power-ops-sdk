from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
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

)


__all__ = [
    "ShopTimeResolution",
    "ShopTimeResolutionWrite",
    "ShopTimeResolutionApply",
    "ShopTimeResolutionList",
    "ShopTimeResolutionWriteList",
    "ShopTimeResolutionApplyList",
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



    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ShopTimeResolution:
        """Convert this GraphQL format of shop time resolution to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ShopTimeResolution(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            minutes_after_start=self.minutes_after_start,
            time_resolution_minutes=self.time_resolution_minutes,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopTimeResolutionWrite:
        """Convert this GraphQL format of shop time resolution to the writing format."""
        return ShopTimeResolutionWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            minutes_after_start=self.minutes_after_start,
            time_resolution_minutes=self.time_resolution_minutes,
        )


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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopTimeResolutionWrite:
        """Convert this read version of shop time resolution to the writing version."""
        return ShopTimeResolutionWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            minutes_after_start=self.minutes_after_start,
            time_resolution_minutes=self.time_resolution_minutes,
        )

    def as_apply(self) -> ShopTimeResolutionWrite:
        """Convert this read version of shop time resolution to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopTimeResolution", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "ShopTimeResolution")
    name: str
    minutes_after_start: list[int] = Field(alias="minutesAfterStart")
    time_resolution_minutes: list[int] = Field(alias="timeResolutionMinutes")


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

        if self.minutes_after_start is not None:
            properties["minutesAfterStart"] = self.minutes_after_start

        if self.time_resolution_minutes is not None:
            properties["timeResolutionMinutes"] = self.time_resolution_minutes

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

        return resources


class ShopTimeResolutionApply(ShopTimeResolutionWrite):
    def __new__(cls, *args, **kwargs) -> ShopTimeResolutionApply:
        warnings.warn(
            "ShopTimeResolutionApply is deprecated and will be removed in v1.0. Use ShopTimeResolutionWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ShopTimeResolution.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)

class ShopTimeResolutionList(DomainModelList[ShopTimeResolution]):
    """List of shop time resolutions in the read version."""

    _INSTANCE = ShopTimeResolution
    def as_write(self) -> ShopTimeResolutionWriteList:
        """Convert these read versions of shop time resolution to the writing versions."""
        return ShopTimeResolutionWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ShopTimeResolutionWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopTimeResolutionWriteList(DomainModelWriteList[ShopTimeResolutionWrite]):
    """List of shop time resolutions in the writing version."""

    _INSTANCE = ShopTimeResolutionWrite

class ShopTimeResolutionApplyList(ShopTimeResolutionWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
    ):

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
