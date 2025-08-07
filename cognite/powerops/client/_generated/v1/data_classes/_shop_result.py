from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    FileMetadata as CogniteFileMetadata,
    FileMetadataWrite as CogniteFileMetadataWrite,
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
)
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._alert import Alert, AlertList, AlertGraphQL, AlertWrite, AlertWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_case import ShopCase, ShopCaseList, ShopCaseGraphQL, ShopCaseWrite, ShopCaseWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_time_series import ShopTimeSeries, ShopTimeSeriesList, ShopTimeSeriesGraphQL, ShopTimeSeriesWrite, ShopTimeSeriesWriteList


__all__ = [
    "ShopResult",
    "ShopResultWrite",
    "ShopResultList",
    "ShopResultWriteList",
    "ShopResultFields",
    "ShopResultTextFields",
    "ShopResultGraphQL",
]


ShopResultTextFields = Literal["external_id", "pre_run", "post_run", "messages", "cplex_logs"]
ShopResultFields = Literal["external_id", "objective_value", "pre_run", "post_run", "messages", "cplex_logs"]

_SHOPRESULT_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "objective_value": "objectiveValue",
    "pre_run": "preRun",
    "post_run": "postRun",
    "messages": "messages",
    "cplex_logs": "cplexLogs",
}


class ShopResultGraphQL(GraphQLCore):
    """This represents the reading version of shop result, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop result.
        data_record: The data record of the shop result node.
        case: The case that was used to produce this result
        objective_value: The sequence of the objective function
        pre_run: The pre-run data for the SHOP run
        post_run: The post-run data for the SHOP run
        messages: The messages from the SHOP run
        cplex_logs: The logs from CPLEX
        alerts: An array of calculation level Alerts.
        output_time_series: TODO
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopResult", "1")
    case: Optional[ShopCaseGraphQL] = Field(default=None, repr=False)
    objective_value: Optional[dict] = Field(None, alias="objectiveValue")
    pre_run: Optional[FileMetadataGraphQL] = Field(None, alias="preRun")
    post_run: Optional[FileMetadataGraphQL] = Field(None, alias="postRun")
    messages: Optional[FileMetadataGraphQL] = None
    cplex_logs: Optional[FileMetadataGraphQL] = Field(None, alias="cplexLogs")
    alerts: Optional[list[AlertGraphQL]] = Field(default=None, repr=False)
    output_time_series: Optional[list[ShopTimeSeriesGraphQL]] = Field(default=None, repr=False, alias="outputTimeSeries")

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


    @field_validator("case", "alerts", "output_time_series", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> ShopResult:
        """Convert this GraphQL format of shop result to the reading format."""
        return ShopResult.model_validate(as_read_args(self))

    def as_write(self) -> ShopResultWrite:
        """Convert this GraphQL format of shop result to the writing format."""
        return ShopResultWrite.model_validate(as_write_args(self))


class ShopResult(DomainModel):
    """This represents the reading version of shop result.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop result.
        data_record: The data record of the shop result node.
        case: The case that was used to produce this result
        objective_value: The sequence of the objective function
        pre_run: The pre-run data for the SHOP run
        post_run: The post-run data for the SHOP run
        messages: The messages from the SHOP run
        cplex_logs: The logs from CPLEX
        alerts: An array of calculation level Alerts.
        output_time_series: TODO
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopResult", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    case: Union[ShopCase, str, dm.NodeId, None] = Field(default=None, repr=False)
    objective_value: Optional[dict] = Field(None, alias="objectiveValue")
    pre_run: Union[FileMetadata, str, None] = Field(None, alias="preRun")
    post_run: Union[FileMetadata, str, None] = Field(None, alias="postRun")
    messages: Union[FileMetadata, str, None] = None
    cplex_logs: Union[FileMetadata, str, None] = Field(None, alias="cplexLogs")
    alerts: Optional[list[Union[Alert, str, dm.NodeId]]] = Field(default=None, repr=False)
    output_time_series: Optional[list[Union[ShopTimeSeries, str, dm.NodeId]]] = Field(default=None, repr=False, alias="outputTimeSeries")
    @field_validator("case", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("alerts", "output_time_series", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> ShopResultWrite:
        """Convert this read version of shop result to the writing version."""
        return ShopResultWrite.model_validate(as_write_args(self))



class ShopResultWrite(DomainModelWrite):
    """This represents the writing version of shop result.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop result.
        data_record: The data record of the shop result node.
        case: The case that was used to produce this result
        objective_value: The sequence of the objective function
        pre_run: The pre-run data for the SHOP run
        post_run: The post-run data for the SHOP run
        messages: The messages from the SHOP run
        cplex_logs: The logs from CPLEX
        alerts: An array of calculation level Alerts.
        output_time_series: TODO
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("case", "cplex_logs", "messages", "objective_value", "post_run", "pre_run",)
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (("alerts", dm.DirectRelationReference("power_ops_types", "calculationIssue")), ("output_time_series", dm.DirectRelationReference("power_ops_types", "ShopResult.outputTimeSeries")),)
    _direct_relations: ClassVar[tuple[str, ...]] = ("case",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopResult", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    case: Union[ShopCaseWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    objective_value: Optional[dict] = Field(None, alias="objectiveValue")
    pre_run: Union[FileMetadataWrite, str, None] = Field(None, alias="preRun")
    post_run: Union[FileMetadataWrite, str, None] = Field(None, alias="postRun")
    messages: Union[FileMetadataWrite, str, None] = None
    cplex_logs: Union[FileMetadataWrite, str, None] = Field(None, alias="cplexLogs")
    alerts: Optional[list[Union[AlertWrite, str, dm.NodeId]]] = Field(default=None, repr=False)
    output_time_series: Optional[list[Union[ShopTimeSeriesWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="outputTimeSeries")

    @field_validator("case", "alerts", "output_time_series", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class ShopResultList(DomainModelList[ShopResult]):
    """List of shop results in the read version."""

    _INSTANCE = ShopResult
    def as_write(self) -> ShopResultWriteList:
        """Convert these read versions of shop result to the writing versions."""
        return ShopResultWriteList([node.as_write() for node in self.data])


    @property
    def case(self) -> ShopCaseList:
        from ._shop_case import ShopCase, ShopCaseList
        return ShopCaseList([item.case for item in self.data if isinstance(item.case, ShopCase)])
    @property
    def alerts(self) -> AlertList:
        from ._alert import Alert, AlertList
        return AlertList([item for items in self.data for item in items.alerts or [] if isinstance(item, Alert)])

    @property
    def output_time_series(self) -> ShopTimeSeriesList:
        from ._shop_time_series import ShopTimeSeries, ShopTimeSeriesList
        return ShopTimeSeriesList([item for items in self.data for item in items.output_time_series or [] if isinstance(item, ShopTimeSeries)])


class ShopResultWriteList(DomainModelWriteList[ShopResultWrite]):
    """List of shop results in the writing version."""

    _INSTANCE = ShopResultWrite
    @property
    def case(self) -> ShopCaseWriteList:
        from ._shop_case import ShopCaseWrite, ShopCaseWriteList
        return ShopCaseWriteList([item.case for item in self.data if isinstance(item.case, ShopCaseWrite)])
    @property
    def alerts(self) -> AlertWriteList:
        from ._alert import AlertWrite, AlertWriteList
        return AlertWriteList([item for items in self.data for item in items.alerts or [] if isinstance(item, AlertWrite)])

    @property
    def output_time_series(self) -> ShopTimeSeriesWriteList:
        from ._shop_time_series import ShopTimeSeriesWrite, ShopTimeSeriesWriteList
        return ShopTimeSeriesWriteList([item for items in self.data for item in items.output_time_series or [] if isinstance(item, ShopTimeSeriesWrite)])



def _create_shop_result_filter(
    view_id: dm.ViewId,
    case: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(case, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(case):
        filters.append(dm.filters.Equals(view_id.as_property_ref("case"), value=as_instance_dict_id(case)))
    if case and isinstance(case, Sequence) and not isinstance(case, str) and not is_tuple_id(case):
        filters.append(dm.filters.In(view_id.as_property_ref("case"), values=[as_instance_dict_id(item) for item in case]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _ShopResultQuery(NodeQueryCore[T_DomainModelList, ShopResultList]):
    _view_id = ShopResult._view_id
    _result_cls = ShopResult
    _result_list_cls_end = ShopResultList

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
        from ._alert import _AlertQuery
        from ._shop_case import _ShopCaseQuery
        from ._shop_time_series import _ShopTimeSeriesQuery

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

        if _ShopCaseQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.case = _ShopCaseQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("case"),
                    direction="outwards",
                ),
                connection_name="case",
                connection_property=ViewPropertyId(self._view_id, "case"),
            )

        if _AlertQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.alerts = _AlertQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="alerts",
                connection_property=ViewPropertyId(self._view_id, "alerts"),
            )

        if _ShopTimeSeriesQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.output_time_series = _ShopTimeSeriesQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="output_time_series",
                connection_property=ViewPropertyId(self._view_id, "outputTimeSeries"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.case_filter = DirectRelationFilter(self, self._view_id.as_property_ref("case"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.case_filter,
        ])

    def list_shop_result(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopResultList:
        return self._list(limit=limit)


class ShopResultQuery(_ShopResultQuery[ShopResultList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopResultList)
