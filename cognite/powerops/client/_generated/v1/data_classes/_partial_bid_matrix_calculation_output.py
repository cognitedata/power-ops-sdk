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
    from cognite.powerops.client._generated.v1.data_classes._bid_configuration_day_ahead import BidConfigurationDayAhead, BidConfigurationDayAheadList, BidConfigurationDayAheadGraphQL, BidConfigurationDayAheadWrite, BidConfigurationDayAheadWriteList
    from cognite.powerops.client._generated.v1.data_classes._bid_matrix import BidMatrix, BidMatrixList, BidMatrixGraphQL, BidMatrixWrite, BidMatrixWriteList
    from cognite.powerops.client._generated.v1.data_classes._partial_bid_matrix_calculation_input import PartialBidMatrixCalculationInput, PartialBidMatrixCalculationInputList, PartialBidMatrixCalculationInputGraphQL, PartialBidMatrixCalculationInputWrite, PartialBidMatrixCalculationInputWriteList


__all__ = [
    "PartialBidMatrixCalculationOutput",
    "PartialBidMatrixCalculationOutputWrite",
    "PartialBidMatrixCalculationOutputApply",
    "PartialBidMatrixCalculationOutputList",
    "PartialBidMatrixCalculationOutputWriteList",
    "PartialBidMatrixCalculationOutputApplyList",
    "PartialBidMatrixCalculationOutputFields",
    "PartialBidMatrixCalculationOutputTextFields",
    "PartialBidMatrixCalculationOutputGraphQL",
]


PartialBidMatrixCalculationOutputTextFields = Literal["external_id", "workflow_execution_id", "function_name", "function_call_id"]
PartialBidMatrixCalculationOutputFields = Literal["external_id", "workflow_execution_id", "workflow_step", "function_name", "function_call_id"]

_PARTIALBIDMATRIXCALCULATIONOUTPUT_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "workflow_execution_id": "workflowExecutionId",
    "workflow_step": "workflowStep",
    "function_name": "functionName",
    "function_call_id": "functionCallId",
}


class PartialBidMatrixCalculationOutputGraphQL(GraphQLCore):
    """This represents the reading version of partial bid matrix calculation output, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the partial bid matrix calculation output.
        data_record: The data record of the partial bid matrix calculation output node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        function_input: The function input field.
        alerts: An array of calculation level Alerts.
        partial_matrix: The partial matrix field.
        bid_configuration: The bid configuration field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PartialBidMatrixCalculationOutput", "1")
    workflow_execution_id: Optional[str] = Field(None, alias="workflowExecutionId")
    workflow_step: Optional[int] = Field(None, alias="workflowStep")
    function_name: Optional[str] = Field(None, alias="functionName")
    function_call_id: Optional[str] = Field(None, alias="functionCallId")
    function_input: Optional[PartialBidMatrixCalculationInputGraphQL] = Field(default=None, repr=False, alias="functionInput")
    alerts: Optional[list[AlertGraphQL]] = Field(default=None, repr=False)
    partial_matrix: Optional[BidMatrixGraphQL] = Field(default=None, repr=False, alias="partialMatrix")
    bid_configuration: Optional[BidConfigurationDayAheadGraphQL] = Field(default=None, repr=False, alias="bidConfiguration")

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


    @field_validator("function_input", "alerts", "partial_matrix", "bid_configuration", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> PartialBidMatrixCalculationOutput:
        """Convert this GraphQL format of partial bid matrix calculation output to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return PartialBidMatrixCalculationOutput(
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
            partial_matrix=self.partial_matrix.as_read()
if isinstance(self.partial_matrix, GraphQLCore)
else self.partial_matrix,
            bid_configuration=self.bid_configuration.as_read()
if isinstance(self.bid_configuration, GraphQLCore)
else self.bid_configuration,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> PartialBidMatrixCalculationOutputWrite:
        """Convert this GraphQL format of partial bid matrix calculation output to the writing format."""
        return PartialBidMatrixCalculationOutputWrite(
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
            partial_matrix=self.partial_matrix.as_write()
if isinstance(self.partial_matrix, GraphQLCore)
else self.partial_matrix,
            bid_configuration=self.bid_configuration.as_write()
if isinstance(self.bid_configuration, GraphQLCore)
else self.bid_configuration,
        )


class PartialBidMatrixCalculationOutput(FunctionOutput):
    """This represents the reading version of partial bid matrix calculation output.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the partial bid matrix calculation output.
        data_record: The data record of the partial bid matrix calculation output node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        function_input: The function input field.
        alerts: An array of calculation level Alerts.
        partial_matrix: The partial matrix field.
        bid_configuration: The bid configuration field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PartialBidMatrixCalculationOutput", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "PartialBidMatrixCalculationOutput")
    partial_matrix: Union[BidMatrix, str, dm.NodeId, None] = Field(default=None, repr=False, alias="partialMatrix")
    bid_configuration: Union[BidConfigurationDayAhead, str, dm.NodeId, None] = Field(default=None, repr=False, alias="bidConfiguration")

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> PartialBidMatrixCalculationOutputWrite:
        """Convert this read version of partial bid matrix calculation output to the writing version."""
        return PartialBidMatrixCalculationOutputWrite(
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
            partial_matrix=self.partial_matrix.as_write()
if isinstance(self.partial_matrix, DomainModel)
else self.partial_matrix,
            bid_configuration=self.bid_configuration.as_write()
if isinstance(self.bid_configuration, DomainModel)
else self.bid_configuration,
        )

    def as_apply(self) -> PartialBidMatrixCalculationOutputWrite:
        """Convert this read version of partial bid matrix calculation output to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()
    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, PartialBidMatrixCalculationOutput],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._alert import Alert
        from ._bid_configuration_day_ahead import BidConfigurationDayAhead
        from ._bid_matrix import BidMatrix
        from ._partial_bid_matrix_calculation_input import PartialBidMatrixCalculationInput
        for instance in instances.values():
            if isinstance(instance.function_input, (dm.NodeId, str)) and (function_input := nodes_by_id.get(instance.function_input)) and isinstance(
                    function_input, PartialBidMatrixCalculationInput
            ):
                instance.function_input = function_input
            if isinstance(instance.partial_matrix, (dm.NodeId, str)) and (partial_matrix := nodes_by_id.get(instance.partial_matrix)) and isinstance(
                    partial_matrix, BidMatrix
            ):
                instance.partial_matrix = partial_matrix
            if isinstance(instance.bid_configuration, (dm.NodeId, str)) and (bid_configuration := nodes_by_id.get(instance.bid_configuration)) and isinstance(
                    bid_configuration, BidConfigurationDayAhead
            ):
                instance.bid_configuration = bid_configuration
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



class PartialBidMatrixCalculationOutputWrite(FunctionOutputWrite):
    """This represents the writing version of partial bid matrix calculation output.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the partial bid matrix calculation output.
        data_record: The data record of the partial bid matrix calculation output node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        function_input: The function input field.
        alerts: An array of calculation level Alerts.
        partial_matrix: The partial matrix field.
        bid_configuration: The bid configuration field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PartialBidMatrixCalculationOutput", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "PartialBidMatrixCalculationOutput")
    partial_matrix: Union[BidMatrixWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="partialMatrix")
    bid_configuration: Union[BidConfigurationDayAheadWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="bidConfiguration")

    @field_validator("partial_matrix", "bid_configuration", mode="before")
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

        if self.partial_matrix is not None:
            properties["partialMatrix"] = {
                "space":  self.space if isinstance(self.partial_matrix, str) else self.partial_matrix.space,
                "externalId": self.partial_matrix if isinstance(self.partial_matrix, str) else self.partial_matrix.external_id,
            }

        if self.bid_configuration is not None:
            properties["bidConfiguration"] = {
                "space":  self.space if isinstance(self.bid_configuration, str) else self.bid_configuration.space,
                "externalId": self.bid_configuration if isinstance(self.bid_configuration, str) else self.bid_configuration.external_id,
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

        if isinstance(self.partial_matrix, DomainModelWrite):
            other_resources = self.partial_matrix._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.bid_configuration, DomainModelWrite):
            other_resources = self.bid_configuration._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class PartialBidMatrixCalculationOutputApply(PartialBidMatrixCalculationOutputWrite):
    def __new__(cls, *args, **kwargs) -> PartialBidMatrixCalculationOutputApply:
        warnings.warn(
            "PartialBidMatrixCalculationOutputApply is deprecated and will be removed in v1.0. Use PartialBidMatrixCalculationOutputWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PartialBidMatrixCalculationOutput.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)

class PartialBidMatrixCalculationOutputList(DomainModelList[PartialBidMatrixCalculationOutput]):
    """List of partial bid matrix calculation outputs in the read version."""

    _INSTANCE = PartialBidMatrixCalculationOutput
    def as_write(self) -> PartialBidMatrixCalculationOutputWriteList:
        """Convert these read versions of partial bid matrix calculation output to the writing versions."""
        return PartialBidMatrixCalculationOutputWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PartialBidMatrixCalculationOutputWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def function_input(self) -> PartialBidMatrixCalculationInputList:
        from ._partial_bid_matrix_calculation_input import PartialBidMatrixCalculationInput, PartialBidMatrixCalculationInputList
        return PartialBidMatrixCalculationInputList([item.function_input for item in self.data if isinstance(item.function_input, PartialBidMatrixCalculationInput)])
    @property
    def alerts(self) -> AlertList:
        from ._alert import Alert, AlertList
        return AlertList([item for items in self.data for item in items.alerts or [] if isinstance(item, Alert)])

    @property
    def partial_matrix(self) -> BidMatrixList:
        from ._bid_matrix import BidMatrix, BidMatrixList
        return BidMatrixList([item.partial_matrix for item in self.data if isinstance(item.partial_matrix, BidMatrix)])
    @property
    def bid_configuration(self) -> BidConfigurationDayAheadList:
        from ._bid_configuration_day_ahead import BidConfigurationDayAhead, BidConfigurationDayAheadList
        return BidConfigurationDayAheadList([item.bid_configuration for item in self.data if isinstance(item.bid_configuration, BidConfigurationDayAhead)])

class PartialBidMatrixCalculationOutputWriteList(DomainModelWriteList[PartialBidMatrixCalculationOutputWrite]):
    """List of partial bid matrix calculation outputs in the writing version."""

    _INSTANCE = PartialBidMatrixCalculationOutputWrite
    @property
    def function_input(self) -> PartialBidMatrixCalculationInputWriteList:
        from ._partial_bid_matrix_calculation_input import PartialBidMatrixCalculationInputWrite, PartialBidMatrixCalculationInputWriteList
        return PartialBidMatrixCalculationInputWriteList([item.function_input for item in self.data if isinstance(item.function_input, PartialBidMatrixCalculationInputWrite)])
    @property
    def alerts(self) -> AlertWriteList:
        from ._alert import AlertWrite, AlertWriteList
        return AlertWriteList([item for items in self.data for item in items.alerts or [] if isinstance(item, AlertWrite)])

    @property
    def partial_matrix(self) -> BidMatrixWriteList:
        from ._bid_matrix import BidMatrixWrite, BidMatrixWriteList
        return BidMatrixWriteList([item.partial_matrix for item in self.data if isinstance(item.partial_matrix, BidMatrixWrite)])
    @property
    def bid_configuration(self) -> BidConfigurationDayAheadWriteList:
        from ._bid_configuration_day_ahead import BidConfigurationDayAheadWrite, BidConfigurationDayAheadWriteList
        return BidConfigurationDayAheadWriteList([item.bid_configuration for item in self.data if isinstance(item.bid_configuration, BidConfigurationDayAheadWrite)])

class PartialBidMatrixCalculationOutputApplyList(PartialBidMatrixCalculationOutputWriteList): ...


def _create_partial_bid_matrix_calculation_output_filter(
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
    partial_matrix: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
    if isinstance(partial_matrix, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(partial_matrix):
        filters.append(dm.filters.Equals(view_id.as_property_ref("partialMatrix"), value=as_instance_dict_id(partial_matrix)))
    if partial_matrix and isinstance(partial_matrix, Sequence) and not isinstance(partial_matrix, str) and not is_tuple_id(partial_matrix):
        filters.append(dm.filters.In(view_id.as_property_ref("partialMatrix"), values=[as_instance_dict_id(item) for item in partial_matrix]))
    if isinstance(bid_configuration, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(bid_configuration):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bidConfiguration"), value=as_instance_dict_id(bid_configuration)))
    if bid_configuration and isinstance(bid_configuration, Sequence) and not isinstance(bid_configuration, str) and not is_tuple_id(bid_configuration):
        filters.append(dm.filters.In(view_id.as_property_ref("bidConfiguration"), values=[as_instance_dict_id(item) for item in bid_configuration]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _PartialBidMatrixCalculationOutputQuery(NodeQueryCore[T_DomainModelList, PartialBidMatrixCalculationOutputList]):
    _view_id = PartialBidMatrixCalculationOutput._view_id
    _result_cls = PartialBidMatrixCalculationOutput
    _result_list_cls_end = PartialBidMatrixCalculationOutputList

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
        from ._bid_configuration_day_ahead import _BidConfigurationDayAheadQuery
        from ._bid_matrix import _BidMatrixQuery
        from ._partial_bid_matrix_calculation_input import _PartialBidMatrixCalculationInputQuery

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

        if _PartialBidMatrixCalculationInputQuery not in created_types:
            self.function_input = _PartialBidMatrixCalculationInputQuery(
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

        if _BidMatrixQuery not in created_types:
            self.partial_matrix = _BidMatrixQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("partialMatrix"),
                    direction="outwards",
                ),
                connection_name="partial_matrix",
            )

        if _BidConfigurationDayAheadQuery not in created_types:
            self.bid_configuration = _BidConfigurationDayAheadQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("bidConfiguration"),
                    direction="outwards",
                ),
                connection_name="bid_configuration",
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

    def list_partial_bid_matrix_calculation_output(self, limit: int = DEFAULT_QUERY_LIMIT) -> PartialBidMatrixCalculationOutputList:
        return self._list(limit=limit)


class PartialBidMatrixCalculationOutputQuery(_PartialBidMatrixCalculationOutputQuery[PartialBidMatrixCalculationOutputList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, PartialBidMatrixCalculationOutputList)
