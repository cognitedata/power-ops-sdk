from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal,  no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    FileMetadata as CogniteFileMetadata,
    FileMetadataWrite as CogniteFileMetadataWrite,
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
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._alert import Alert, AlertList, AlertGraphQL, AlertWrite, AlertWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_case import ShopCase, ShopCaseList, ShopCaseGraphQL, ShopCaseWrite, ShopCaseWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_time_series import ShopTimeSeries, ShopTimeSeriesList, ShopTimeSeriesGraphQL, ShopTimeSeriesWrite, ShopTimeSeriesWriteList


__all__ = [
    "ShopResult",
    "ShopResultWrite",
    "ShopResultApply",
    "ShopResultList",
    "ShopResultWriteList",
    "ShopResultApplyList",
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ShopResult:
        """Convert this GraphQL format of shop result to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ShopResult(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            case=self.case.as_read()
if isinstance(self.case, GraphQLCore)
else self.case,
            objective_value=self.objective_value,
            pre_run=self.pre_run.as_read() if self.pre_run else None,
            post_run=self.post_run.as_read() if self.post_run else None,
            messages=self.messages.as_read() if self.messages else None,
            cplex_logs=self.cplex_logs.as_read() if self.cplex_logs else None,
            alerts=[alert.as_read() for alert in self.alerts] if self.alerts is not None else None,
            output_time_series=[output_time_series.as_read() for output_time_series in self.output_time_series] if self.output_time_series is not None else None,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopResultWrite:
        """Convert this GraphQL format of shop result to the writing format."""
        return ShopResultWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            case=self.case.as_write()
if isinstance(self.case, GraphQLCore)
else self.case,
            objective_value=self.objective_value,
            pre_run=self.pre_run.as_write() if self.pre_run else None,
            post_run=self.post_run.as_write() if self.post_run else None,
            messages=self.messages.as_write() if self.messages else None,
            cplex_logs=self.cplex_logs.as_write() if self.cplex_logs else None,
            alerts=[alert.as_write() for alert in self.alerts] if self.alerts is not None else None,
            output_time_series=[output_time_series.as_write() for output_time_series in self.output_time_series] if self.output_time_series is not None else None,
        )


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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopResultWrite:
        """Convert this read version of shop result to the writing version."""
        return ShopResultWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            case=self.case.as_write()
if isinstance(self.case, DomainModel)
else self.case,
            objective_value=self.objective_value,
            pre_run=self.pre_run.as_write() if isinstance(self.pre_run, CogniteFileMetadata) else self.pre_run,
            post_run=self.post_run.as_write() if isinstance(self.post_run, CogniteFileMetadata) else self.post_run,
            messages=self.messages.as_write() if isinstance(self.messages, CogniteFileMetadata) else self.messages,
            cplex_logs=self.cplex_logs.as_write() if isinstance(self.cplex_logs, CogniteFileMetadata) else self.cplex_logs,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts] if self.alerts is not None else None,
            output_time_series=[output_time_series.as_write() if isinstance(output_time_series, DomainModel) else output_time_series for output_time_series in self.output_time_series] if self.output_time_series is not None else None,
        )

    def as_apply(self) -> ShopResultWrite:
        """Convert this read version of shop result to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()
    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, ShopResult],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._alert import Alert
        from ._shop_case import ShopCase
        from ._shop_time_series import ShopTimeSeries
        for instance in instances.values():
            if isinstance(instance.case, (dm.NodeId, str)) and (case := nodes_by_id.get(instance.case)) and isinstance(
                    case, ShopCase
            ):
                instance.case = case
            if edges := edges_by_source_node.get(instance.as_id()):
                alerts: list[Alert | str | dm.NodeId] = []
                output_time_series: list[ShopTimeSeries | str | dm.NodeId] = []
                for edge in edges:
                    value: DomainModel | DomainRelation | str | dm.NodeId
                    if isinstance(edge, DomainRelation):
                        value = edge
                    else:
                        other_end: dm.DirectRelationReference = (
                            edge.end_node
                            if edge.start_node.space == instance.space
                            and edge.start_node.external_id == instance.external_id
                            else edge.start_node
                        )
                        destination: dm.NodeId | str = (
                            as_node_id(other_end)
                            if other_end.space != DEFAULT_INSTANCE_SPACE
                            else other_end.external_id
                        )
                        if destination in nodes_by_id:
                            value = nodes_by_id[destination]
                        else:
                            value = destination
                    edge_type = edge.edge_type if isinstance(edge, DomainRelation) else edge.type

                    if edge_type == dm.DirectRelationReference("power_ops_types", "calculationIssue") and isinstance(
                        value, (Alert, str, dm.NodeId)
                    ):
                        alerts.append(value)
                    if edge_type == dm.DirectRelationReference("power_ops_types", "ShopResult.outputTimeSeries") and isinstance(
                        value, (ShopTimeSeries, str, dm.NodeId)
                    ):
                        output_time_series.append(value)

                instance.alerts = alerts or None
                instance.output_time_series = output_time_series or None



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

        if self.case is not None:
            properties["case"] = {
                "space":  self.space if isinstance(self.case, str) else self.case.space,
                "externalId": self.case if isinstance(self.case, str) else self.case.external_id,
            }

        if self.objective_value is not None or write_none:
            properties["objectiveValue"] = self.objective_value

        if self.pre_run is not None or write_none:
            properties["preRun"] = self.pre_run if isinstance(self.pre_run, str) or self.pre_run is None else self.pre_run.external_id

        if self.post_run is not None or write_none:
            properties["postRun"] = self.post_run if isinstance(self.post_run, str) or self.post_run is None else self.post_run.external_id

        if self.messages is not None or write_none:
            properties["messages"] = self.messages if isinstance(self.messages, str) or self.messages is None else self.messages.external_id

        if self.cplex_logs is not None or write_none:
            properties["cplexLogs"] = self.cplex_logs if isinstance(self.cplex_logs, str) or self.cplex_logs is None else self.cplex_logs.external_id

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

        edge_type = dm.DirectRelationReference("power_ops_types", "calculationIssue")
        for alert in self.alerts or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=alert,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("power_ops_types", "ShopResult.outputTimeSeries")
        for output_time_series in self.output_time_series or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=output_time_series,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.case, DomainModelWrite):
            other_resources = self.case._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.pre_run, CogniteFileMetadataWrite):
            resources.files.append(self.pre_run)

        if isinstance(self.post_run, CogniteFileMetadataWrite):
            resources.files.append(self.post_run)

        if isinstance(self.messages, CogniteFileMetadataWrite):
            resources.files.append(self.messages)

        if isinstance(self.cplex_logs, CogniteFileMetadataWrite):
            resources.files.append(self.cplex_logs)

        return resources


class ShopResultApply(ShopResultWrite):
    def __new__(cls, *args, **kwargs) -> ShopResultApply:
        warnings.warn(
            "ShopResultApply is deprecated and will be removed in v1.0. Use ShopResultWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ShopResult.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)

class ShopResultList(DomainModelList[ShopResult]):
    """List of shop results in the read version."""

    _INSTANCE = ShopResult
    def as_write(self) -> ShopResultWriteList:
        """Convert these read versions of shop result to the writing versions."""
        return ShopResultWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ShopResultWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

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


class ShopResultApplyList(ShopResultWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
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
            connection_type,
            reverse_expression,
        )

        if _ShopCaseQuery not in created_types:
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
            )

        if _AlertQuery not in created_types:
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
            )

        if _ShopTimeSeriesQuery not in created_types:
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
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])

    def list_shop_result(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopResultList:
        return self._list(limit=limit)


class ShopResultQuery(_ShopResultQuery[ShopResultList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopResultList)
