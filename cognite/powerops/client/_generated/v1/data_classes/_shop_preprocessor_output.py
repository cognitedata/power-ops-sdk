from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal,  no_type_check, Optional, Union

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
    IntFilter,
)
from cognite.powerops.client._generated.v1.data_classes._function_output import FunctionOutput, FunctionOutputWrite
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._alert import Alert, AlertList, AlertGraphQL, AlertWrite, AlertWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_case import ShopCase, ShopCaseList, ShopCaseGraphQL, ShopCaseWrite, ShopCaseWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_preprocessor_input import ShopPreprocessorInput, ShopPreprocessorInputList, ShopPreprocessorInputGraphQL, ShopPreprocessorInputWrite, ShopPreprocessorInputWriteList


__all__ = [
    "ShopPreprocessorOutput",
    "ShopPreprocessorOutputWrite",
    "ShopPreprocessorOutputApply",
    "ShopPreprocessorOutputList",
    "ShopPreprocessorOutputWriteList",
    "ShopPreprocessorOutputApplyList",
    "ShopPreprocessorOutputFields",
    "ShopPreprocessorOutputTextFields",
    "ShopPreprocessorOutputGraphQL",
]


ShopPreprocessorOutputTextFields = Literal["external_id", "workflow_execution_id", "function_name", "function_call_id"]
ShopPreprocessorOutputFields = Literal["external_id", "workflow_execution_id", "workflow_step", "function_name", "function_call_id"]

_SHOPPREPROCESSOROUTPUT_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "workflow_execution_id": "workflowExecutionId",
    "workflow_step": "workflowStep",
    "function_name": "functionName",
    "function_call_id": "functionCallId",
}


class ShopPreprocessorOutputGraphQL(GraphQLCore):
    """This represents the reading version of shop preprocessor output, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop preprocessor output.
        data_record: The data record of the shop preprocessor output node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        function_input: The function input field.
        alerts: An array of calculation level Alerts.
        case: The Case to trigger shop with
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopPreprocessorOutput", "1")
    workflow_execution_id: Optional[str] = Field(None, alias="workflowExecutionId")
    workflow_step: Optional[int] = Field(None, alias="workflowStep")
    function_name: Optional[str] = Field(None, alias="functionName")
    function_call_id: Optional[str] = Field(None, alias="functionCallId")
    function_input: Optional[ShopPreprocessorInputGraphQL] = Field(default=None, repr=False, alias="functionInput")
    alerts: Optional[list[AlertGraphQL]] = Field(default=None, repr=False)
    case: Optional[ShopCaseGraphQL] = Field(default=None, repr=False)

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


    @field_validator("function_input", "alerts", "case", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ShopPreprocessorOutput:
        """Convert this GraphQL format of shop preprocessor output to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ShopPreprocessorOutput(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            workflow_execution_id=self.workflow_execution_id,
            workflow_step=self.workflow_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            function_input=self.function_input.as_read()
if isinstance(self.function_input, GraphQLCore)
else self.function_input,
            alerts=[alert.as_read() for alert in self.alerts] if self.alerts is not None else None,
            case=self.case.as_read()
if isinstance(self.case, GraphQLCore)
else self.case,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopPreprocessorOutputWrite:
        """Convert this GraphQL format of shop preprocessor output to the writing format."""
        return ShopPreprocessorOutputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            workflow_execution_id=self.workflow_execution_id,
            workflow_step=self.workflow_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            function_input=self.function_input.as_write()
if isinstance(self.function_input, GraphQLCore)
else self.function_input,
            alerts=[alert.as_write() for alert in self.alerts] if self.alerts is not None else None,
            case=self.case.as_write()
if isinstance(self.case, GraphQLCore)
else self.case,
        )


class ShopPreprocessorOutput(FunctionOutput):
    """This represents the reading version of shop preprocessor output.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop preprocessor output.
        data_record: The data record of the shop preprocessor output node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        function_input: The function input field.
        alerts: An array of calculation level Alerts.
        case: The Case to trigger shop with
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopPreprocessorOutput", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopPreprocessorOutput")
    case: Union[ShopCase, str, dm.NodeId, None] = Field(default=None, repr=False)

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopPreprocessorOutputWrite:
        """Convert this read version of shop preprocessor output to the writing version."""
        return ShopPreprocessorOutputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            workflow_execution_id=self.workflow_execution_id,
            workflow_step=self.workflow_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            function_input=self.function_input.as_write()
if isinstance(self.function_input, DomainModel)
else self.function_input,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts] if self.alerts is not None else None,
            case=self.case.as_write()
if isinstance(self.case, DomainModel)
else self.case,
        )

    def as_apply(self) -> ShopPreprocessorOutputWrite:
        """Convert this read version of shop preprocessor output to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()
    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, ShopPreprocessorOutput],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._alert import Alert
        from ._shop_case import ShopCase
        from ._shop_preprocessor_input import ShopPreprocessorInput
        for instance in instances.values():
            if isinstance(instance.function_input, (dm.NodeId, str)) and (function_input := nodes_by_id.get(instance.function_input)) and isinstance(
                    function_input, ShopPreprocessorInput
            ):
                instance.function_input = function_input
            if isinstance(instance.case, (dm.NodeId, str)) and (case := nodes_by_id.get(instance.case)) and isinstance(
                    case, ShopCase
            ):
                instance.case = case
            if edges := edges_by_source_node.get(instance.as_id()):
                alerts: list[Alert | str | dm.NodeId] = []
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

                instance.alerts = alerts or None



class ShopPreprocessorOutputWrite(FunctionOutputWrite):
    """This represents the writing version of shop preprocessor output.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop preprocessor output.
        data_record: The data record of the shop preprocessor output node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        function_input: The function input field.
        alerts: An array of calculation level Alerts.
        case: The Case to trigger shop with
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopPreprocessorOutput", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "ShopPreprocessorOutput")
    case: Union[ShopCaseWrite, str, dm.NodeId, None] = Field(default=None, repr=False)

    @field_validator("case", mode="before")
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

        if self.workflow_execution_id is not None:
            properties["workflowExecutionId"] = self.workflow_execution_id

        if self.workflow_step is not None:
            properties["workflowStep"] = self.workflow_step

        if self.function_name is not None:
            properties["functionName"] = self.function_name

        if self.function_call_id is not None:
            properties["functionCallId"] = self.function_call_id

        if self.function_input is not None:
            properties["functionInput"] = {
                "space":  self.space if isinstance(self.function_input, str) else self.function_input.space,
                "externalId": self.function_input if isinstance(self.function_input, str) else self.function_input.external_id,
            }

        if self.case is not None:
            properties["case"] = {
                "space":  self.space if isinstance(self.case, str) else self.case.space,
                "externalId": self.case if isinstance(self.case, str) else self.case.external_id,
            }

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

        if isinstance(self.function_input, DomainModelWrite):
            other_resources = self.function_input._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.case, DomainModelWrite):
            other_resources = self.case._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class ShopPreprocessorOutputApply(ShopPreprocessorOutputWrite):
    def __new__(cls, *args, **kwargs) -> ShopPreprocessorOutputApply:
        warnings.warn(
            "ShopPreprocessorOutputApply is deprecated and will be removed in v1.0. Use ShopPreprocessorOutputWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ShopPreprocessorOutput.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)

class ShopPreprocessorOutputList(DomainModelList[ShopPreprocessorOutput]):
    """List of shop preprocessor outputs in the read version."""

    _INSTANCE = ShopPreprocessorOutput
    def as_write(self) -> ShopPreprocessorOutputWriteList:
        """Convert these read versions of shop preprocessor output to the writing versions."""
        return ShopPreprocessorOutputWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ShopPreprocessorOutputWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def function_input(self) -> ShopPreprocessorInputList:
        from ._shop_preprocessor_input import ShopPreprocessorInput, ShopPreprocessorInputList
        return ShopPreprocessorInputList([item.function_input for item in self.data if isinstance(item.function_input, ShopPreprocessorInput)])
    @property
    def alerts(self) -> AlertList:
        from ._alert import Alert, AlertList
        return AlertList([item for items in self.data for item in items.alerts or [] if isinstance(item, Alert)])

    @property
    def case(self) -> ShopCaseList:
        from ._shop_case import ShopCase, ShopCaseList
        return ShopCaseList([item.case for item in self.data if isinstance(item.case, ShopCase)])

class ShopPreprocessorOutputWriteList(DomainModelWriteList[ShopPreprocessorOutputWrite]):
    """List of shop preprocessor outputs in the writing version."""

    _INSTANCE = ShopPreprocessorOutputWrite
    @property
    def function_input(self) -> ShopPreprocessorInputWriteList:
        from ._shop_preprocessor_input import ShopPreprocessorInputWrite, ShopPreprocessorInputWriteList
        return ShopPreprocessorInputWriteList([item.function_input for item in self.data if isinstance(item.function_input, ShopPreprocessorInputWrite)])
    @property
    def alerts(self) -> AlertWriteList:
        from ._alert import AlertWrite, AlertWriteList
        return AlertWriteList([item for items in self.data for item in items.alerts or [] if isinstance(item, AlertWrite)])

    @property
    def case(self) -> ShopCaseWriteList:
        from ._shop_case import ShopCaseWrite, ShopCaseWriteList
        return ShopCaseWriteList([item.case for item in self.data if isinstance(item.case, ShopCaseWrite)])

class ShopPreprocessorOutputApplyList(ShopPreprocessorOutputWriteList): ...


def _create_shop_preprocessor_output_filter(
    view_id: dm.ViewId,
    workflow_execution_id: str | list[str] | None = None,
    workflow_execution_id_prefix: str | None = None,
    min_workflow_step: int | None = None,
    max_workflow_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
    function_call_id: str | list[str] | None = None,
    function_call_id_prefix: str | None = None,
    function_input: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    case: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(workflow_execution_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("workflowExecutionId"), value=workflow_execution_id))
    if workflow_execution_id and isinstance(workflow_execution_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("workflowExecutionId"), values=workflow_execution_id))
    if workflow_execution_id_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("workflowExecutionId"), value=workflow_execution_id_prefix))
    if min_workflow_step is not None or max_workflow_step is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("workflowStep"), gte=min_workflow_step, lte=max_workflow_step))
    if isinstance(function_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("functionName"), value=function_name))
    if function_name and isinstance(function_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("functionName"), values=function_name))
    if function_name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("functionName"), value=function_name_prefix))
    if isinstance(function_call_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("functionCallId"), value=function_call_id))
    if function_call_id and isinstance(function_call_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("functionCallId"), values=function_call_id))
    if function_call_id_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("functionCallId"), value=function_call_id_prefix))
    if isinstance(function_input, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(function_input):
        filters.append(dm.filters.Equals(view_id.as_property_ref("functionInput"), value=as_instance_dict_id(function_input)))
    if function_input and isinstance(function_input, Sequence) and not isinstance(function_input, str) and not is_tuple_id(function_input):
        filters.append(dm.filters.In(view_id.as_property_ref("functionInput"), values=[as_instance_dict_id(item) for item in function_input]))
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


class _ShopPreprocessorOutputQuery(NodeQueryCore[T_DomainModelList, ShopPreprocessorOutputList]):
    _view_id = ShopPreprocessorOutput._view_id
    _result_cls = ShopPreprocessorOutput
    _result_list_cls_end = ShopPreprocessorOutputList

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
        from ._shop_preprocessor_input import _ShopPreprocessorInputQuery

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

        if _ShopPreprocessorInputQuery not in created_types:
            self.function_input = _ShopPreprocessorInputQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("functionInput"),
                    direction="outwards",
                ),
                connection_name="function_input",
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

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.workflow_execution_id = StringFilter(self, self._view_id.as_property_ref("workflowExecutionId"))
        self.workflow_step = IntFilter(self, self._view_id.as_property_ref("workflowStep"))
        self.function_name = StringFilter(self, self._view_id.as_property_ref("functionName"))
        self.function_call_id = StringFilter(self, self._view_id.as_property_ref("functionCallId"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.workflow_execution_id,
            self.workflow_step,
            self.function_name,
            self.function_call_id,
        ])

    def list_shop_preprocessor_output(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopPreprocessorOutputList:
        return self._list(limit=limit)


class ShopPreprocessorOutputQuery(_ShopPreprocessorOutputQuery[ShopPreprocessorOutputList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopPreprocessorOutputList)
