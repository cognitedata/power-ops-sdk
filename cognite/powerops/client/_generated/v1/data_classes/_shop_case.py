from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

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
    DirectRelationFilter,
    TimestampFilter,
)
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._shop_file import ShopFile, ShopFileList, ShopFileGraphQL, ShopFileWrite, ShopFileWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_scenario import ShopScenario, ShopScenarioList, ShopScenarioGraphQL, ShopScenarioWrite, ShopScenarioWriteList


__all__ = [
    "ShopCase",
    "ShopCaseWrite",
    "ShopCaseList",
    "ShopCaseWriteList",
    "ShopCaseFields",
    "ShopCaseGraphQL",
]


ShopCaseTextFields = Literal["external_id", ]
ShopCaseFields = Literal["external_id", "start_time", "end_time", "status"]

_SHOPCASE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "start_time": "startTime",
    "end_time": "endTime",
    "status": "status",
}


class ShopCaseGraphQL(GraphQLCore):
    """This represents the reading version of shop case, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop case.
        data_record: The data record of the shop case node.
        scenario: The Shop scenario that was used to produce this result
        start_time: The start time of the case
        end_time: The end time of the case
        status: The status of the ShopCase
        shop_files: The list of shop files that are used in a shop run. This encompasses all shop files such as case,
            module series, cut files etc.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopCase", "1")
    scenario: Optional[ShopScenarioGraphQL] = Field(default=None, repr=False)
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")
    status: Optional[Literal["completed", "default", "failed", "notSet", "queued", "running", "stale", "timedOut", "triggered"]] = None
    shop_files: Optional[list[ShopFileGraphQL]] = Field(default=None, repr=False, alias="shopFiles")

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


    @field_validator("scenario", "shop_files", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> ShopCase:
        """Convert this GraphQL format of shop case to the reading format."""
        return ShopCase.model_validate(as_read_args(self))

    def as_write(self) -> ShopCaseWrite:
        """Convert this GraphQL format of shop case to the writing format."""
        return ShopCaseWrite.model_validate(as_write_args(self))


class ShopCase(DomainModel):
    """This represents the reading version of shop case.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop case.
        data_record: The data record of the shop case node.
        scenario: The Shop scenario that was used to produce this result
        start_time: The start time of the case
        end_time: The end time of the case
        status: The status of the ShopCase
        shop_files: The list of shop files that are used in a shop run. This encompasses all shop files such as case,
            module series, cut files etc.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopCase", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    scenario: Union[ShopScenario, str, dm.NodeId, None] = Field(default=None, repr=False)
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")
    status: Optional[Literal["completed", "default", "failed", "notSet", "queued", "running", "stale", "timedOut", "triggered"]] | str = None
    shop_files: Optional[list[Union[ShopFile, str, dm.NodeId]]] = Field(default=None, repr=False, alias="shopFiles")
    @field_validator("scenario", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("shop_files", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> ShopCaseWrite:
        """Convert this read version of shop case to the writing version."""
        return ShopCaseWrite.model_validate(as_write_args(self))



class ShopCaseWrite(DomainModelWrite):
    """This represents the writing version of shop case.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop case.
        data_record: The data record of the shop case node.
        scenario: The Shop scenario that was used to produce this result
        start_time: The start time of the case
        end_time: The end time of the case
        status: The status of the ShopCase
        shop_files: The list of shop files that are used in a shop run. This encompasses all shop files such as case,
            module series, cut files etc.
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("end_time", "scenario", "start_time", "status",)
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (("shop_files", dm.DirectRelationReference("power_ops_types", "ShopCase.shopFiles")),)
    _direct_relations: ClassVar[tuple[str, ...]] = ("scenario",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopCase", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    scenario: Union[ShopScenarioWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")
    status: Optional[Literal["completed", "default", "failed", "notSet", "queued", "running", "stale", "timedOut", "triggered"]] = None
    shop_files: Optional[list[Union[ShopFileWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="shopFiles")

    @field_validator("scenario", "shop_files", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class ShopCaseList(DomainModelList[ShopCase]):
    """List of shop cases in the read version."""

    _INSTANCE = ShopCase
    def as_write(self) -> ShopCaseWriteList:
        """Convert these read versions of shop case to the writing versions."""
        return ShopCaseWriteList([node.as_write() for node in self.data])


    @property
    def scenario(self) -> ShopScenarioList:
        from ._shop_scenario import ShopScenario, ShopScenarioList
        return ShopScenarioList([item.scenario for item in self.data if isinstance(item.scenario, ShopScenario)])
    @property
    def shop_files(self) -> ShopFileList:
        from ._shop_file import ShopFile, ShopFileList
        return ShopFileList([item for items in self.data for item in items.shop_files or [] if isinstance(item, ShopFile)])


class ShopCaseWriteList(DomainModelWriteList[ShopCaseWrite]):
    """List of shop cases in the writing version."""

    _INSTANCE = ShopCaseWrite
    @property
    def scenario(self) -> ShopScenarioWriteList:
        from ._shop_scenario import ShopScenarioWrite, ShopScenarioWriteList
        return ShopScenarioWriteList([item.scenario for item in self.data if isinstance(item.scenario, ShopScenarioWrite)])
    @property
    def shop_files(self) -> ShopFileWriteList:
        from ._shop_file import ShopFileWrite, ShopFileWriteList
        return ShopFileWriteList([item for items in self.data for item in items.shop_files or [] if isinstance(item, ShopFileWrite)])



def _create_shop_case_filter(
    view_id: dm.ViewId,
    scenario: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    min_start_time: datetime.datetime | None = None,
    max_start_time: datetime.datetime | None = None,
    min_end_time: datetime.datetime | None = None,
    max_end_time: datetime.datetime | None = None,
    status: Literal["completed", "default", "failed", "notSet", "queued", "running", "stale", "timedOut", "triggered"] | list[Literal["completed", "default", "failed", "notSet", "queued", "running", "stale", "timedOut", "triggered"]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(scenario, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(scenario):
        filters.append(dm.filters.Equals(view_id.as_property_ref("scenario"), value=as_instance_dict_id(scenario)))
    if scenario and isinstance(scenario, Sequence) and not isinstance(scenario, str) and not is_tuple_id(scenario):
        filters.append(dm.filters.In(view_id.as_property_ref("scenario"), values=[as_instance_dict_id(item) for item in scenario]))
    if min_start_time is not None or max_start_time is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("startTime"), gte=min_start_time.isoformat(timespec="milliseconds") if min_start_time else None, lte=max_start_time.isoformat(timespec="milliseconds") if max_start_time else None))
    if min_end_time is not None or max_end_time is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("endTime"), gte=min_end_time.isoformat(timespec="milliseconds") if min_end_time else None, lte=max_end_time.isoformat(timespec="milliseconds") if max_end_time else None))
    if isinstance(status, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("status"), value=status))
    if status and isinstance(status, list):
        filters.append(dm.filters.In(view_id.as_property_ref("status"), values=status))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _ShopCaseQuery(NodeQueryCore[T_DomainModelList, ShopCaseList]):
    _view_id = ShopCase._view_id
    _result_cls = ShopCase
    _result_list_cls_end = ShopCaseList

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
        from ._shop_file import _ShopFileQuery
        from ._shop_scenario import _ShopScenarioQuery

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

        if _ShopScenarioQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.scenario = _ShopScenarioQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("scenario"),
                    direction="outwards",
                ),
                connection_name="scenario",
                connection_property=ViewPropertyId(self._view_id, "scenario"),
            )

        if _ShopFileQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.shop_files = _ShopFileQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="shop_files",
                connection_property=ViewPropertyId(self._view_id, "shopFiles"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.scenario_filter = DirectRelationFilter(self, self._view_id.as_property_ref("scenario"))
        self.start_time = TimestampFilter(self, self._view_id.as_property_ref("startTime"))
        self.end_time = TimestampFilter(self, self._view_id.as_property_ref("endTime"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.scenario_filter,
            self.start_time,
            self.end_time,
        ])

    def list_shop_case(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopCaseList:
        return self._list(limit=limit)


class ShopCaseQuery(_ShopCaseQuery[ShopCaseList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopCaseList)
