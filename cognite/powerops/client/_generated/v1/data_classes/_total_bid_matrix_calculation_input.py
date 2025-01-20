from __future__ import annotations

import datetime
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
    DateFilter,
    IntFilter,
)
from cognite.powerops.client._generated.v1.data_classes._function_input import FunctionInput, FunctionInputWrite
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._bid_configuration_day_ahead import BidConfigurationDayAhead, BidConfigurationDayAheadList, BidConfigurationDayAheadGraphQL, BidConfigurationDayAheadWrite, BidConfigurationDayAheadWriteList
    from cognite.powerops.client._generated.v1.data_classes._bid_matrix import BidMatrix, BidMatrixList, BidMatrixGraphQL, BidMatrixWrite, BidMatrixWriteList


__all__ = [
    "TotalBidMatrixCalculationInput",
    "TotalBidMatrixCalculationInputWrite",
    "TotalBidMatrixCalculationInputApply",
    "TotalBidMatrixCalculationInputList",
    "TotalBidMatrixCalculationInputWriteList",
    "TotalBidMatrixCalculationInputApplyList",
    "TotalBidMatrixCalculationInputFields",
    "TotalBidMatrixCalculationInputTextFields",
    "TotalBidMatrixCalculationInputGraphQL",
]


TotalBidMatrixCalculationInputTextFields = Literal["external_id", "workflow_execution_id", "function_name", "function_call_id"]
TotalBidMatrixCalculationInputFields = Literal["external_id", "workflow_execution_id", "workflow_step", "function_name", "function_call_id", "bid_date"]

_TOTALBIDMATRIXCALCULATIONINPUT_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "workflow_execution_id": "workflowExecutionId",
    "workflow_step": "workflowStep",
    "function_name": "functionName",
    "function_call_id": "functionCallId",
    "bid_date": "bidDate",
}


class TotalBidMatrixCalculationInputGraphQL(GraphQLCore):
    """This represents the reading version of total bid matrix calculation input, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the total bid matrix calculation input.
        data_record: The data record of the total bid matrix calculation input node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        bid_configuration: The bid configuration field.
        bid_date: The bid date
        partial_bid_matrices: The partial bid matrices that are used to calculate the total bid matrix.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "TotalBidMatrixCalculationInput", "1")
    workflow_execution_id: Optional[str] = Field(None, alias="workflowExecutionId")
    workflow_step: Optional[int] = Field(None, alias="workflowStep")
    function_name: Optional[str] = Field(None, alias="functionName")
    function_call_id: Optional[str] = Field(None, alias="functionCallId")
    bid_configuration: Optional[BidConfigurationDayAheadGraphQL] = Field(default=None, repr=False, alias="bidConfiguration")
    bid_date: Optional[datetime.date] = Field(None, alias="bidDate")
    partial_bid_matrices: Optional[list[BidMatrixGraphQL]] = Field(default=None, repr=False, alias="partialBidMatrices")

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


    @field_validator("bid_configuration", "partial_bid_matrices", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> TotalBidMatrixCalculationInput:
        """Convert this GraphQL format of total bid matrix calculation input to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return TotalBidMatrixCalculationInput(
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
            bid_configuration=self.bid_configuration.as_read()
if isinstance(self.bid_configuration, GraphQLCore)
else self.bid_configuration,
            bid_date=self.bid_date,
            partial_bid_matrices=[partial_bid_matrice.as_read() for partial_bid_matrice in self.partial_bid_matrices] if self.partial_bid_matrices is not None else None,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> TotalBidMatrixCalculationInputWrite:
        """Convert this GraphQL format of total bid matrix calculation input to the writing format."""
        return TotalBidMatrixCalculationInputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            workflow_execution_id=self.workflow_execution_id,
            workflow_step=self.workflow_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            bid_configuration=self.bid_configuration.as_write()
if isinstance(self.bid_configuration, GraphQLCore)
else self.bid_configuration,
            bid_date=self.bid_date,
            partial_bid_matrices=[partial_bid_matrice.as_write() for partial_bid_matrice in self.partial_bid_matrices] if self.partial_bid_matrices is not None else None,
        )


class TotalBidMatrixCalculationInput(FunctionInput):
    """This represents the reading version of total bid matrix calculation input.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the total bid matrix calculation input.
        data_record: The data record of the total bid matrix calculation input node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        bid_configuration: The bid configuration field.
        bid_date: The bid date
        partial_bid_matrices: The partial bid matrices that are used to calculate the total bid matrix.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "TotalBidMatrixCalculationInput", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "TotalBidMatrixCalculationInput")
    bid_configuration: Union[BidConfigurationDayAhead, str, dm.NodeId, None] = Field(default=None, repr=False, alias="bidConfiguration")
    bid_date: Optional[datetime.date] = Field(None, alias="bidDate")
    partial_bid_matrices: Optional[list[Union[BidMatrix, str, dm.NodeId]]] = Field(default=None, repr=False, alias="partialBidMatrices")

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> TotalBidMatrixCalculationInputWrite:
        """Convert this read version of total bid matrix calculation input to the writing version."""
        return TotalBidMatrixCalculationInputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            workflow_execution_id=self.workflow_execution_id,
            workflow_step=self.workflow_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            bid_configuration=self.bid_configuration.as_write()
if isinstance(self.bid_configuration, DomainModel)
else self.bid_configuration,
            bid_date=self.bid_date,
            partial_bid_matrices=[partial_bid_matrice.as_write() if isinstance(partial_bid_matrice, DomainModel) else partial_bid_matrice for partial_bid_matrice in self.partial_bid_matrices] if self.partial_bid_matrices is not None else None,
        )

    def as_apply(self) -> TotalBidMatrixCalculationInputWrite:
        """Convert this read version of total bid matrix calculation input to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()
    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, TotalBidMatrixCalculationInput],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._bid_configuration_day_ahead import BidConfigurationDayAhead
        from ._bid_matrix import BidMatrix
        for instance in instances.values():
            if isinstance(instance.bid_configuration, (dm.NodeId, str)) and (bid_configuration := nodes_by_id.get(instance.bid_configuration)) and isinstance(
                    bid_configuration, BidConfigurationDayAhead
            ):
                instance.bid_configuration = bid_configuration
            if edges := edges_by_source_node.get(instance.as_id()):
                partial_bid_matrices: list[BidMatrix | str | dm.NodeId] = []
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

                    if edge_type == dm.DirectRelationReference("power_ops_types", "BidMatrix") and isinstance(
                        value, (BidMatrix, str, dm.NodeId)
                    ):
                        partial_bid_matrices.append(value)

                instance.partial_bid_matrices = partial_bid_matrices or None



class TotalBidMatrixCalculationInputWrite(FunctionInputWrite):
    """This represents the writing version of total bid matrix calculation input.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the total bid matrix calculation input.
        data_record: The data record of the total bid matrix calculation input node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        bid_configuration: The bid configuration field.
        bid_date: The bid date
        partial_bid_matrices: The partial bid matrices that are used to calculate the total bid matrix.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "TotalBidMatrixCalculationInput", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "TotalBidMatrixCalculationInput")
    bid_configuration: Union[BidConfigurationDayAheadWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="bidConfiguration")
    bid_date: Optional[datetime.date] = Field(None, alias="bidDate")
    partial_bid_matrices: Optional[list[Union[BidMatrixWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="partialBidMatrices")

    @field_validator("bid_configuration", "partial_bid_matrices", mode="before")
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

        if self.bid_configuration is not None:
            properties["bidConfiguration"] = {
                "space":  self.space if isinstance(self.bid_configuration, str) else self.bid_configuration.space,
                "externalId": self.bid_configuration if isinstance(self.bid_configuration, str) else self.bid_configuration.external_id,
            }

        if self.bid_date is not None or write_none:
            properties["bidDate"] = self.bid_date.isoformat() if self.bid_date else None

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

        edge_type = dm.DirectRelationReference("power_ops_types", "BidMatrix")
        for partial_bid_matrice in self.partial_bid_matrices or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=partial_bid_matrice,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.bid_configuration, DomainModelWrite):
            other_resources = self.bid_configuration._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class TotalBidMatrixCalculationInputApply(TotalBidMatrixCalculationInputWrite):
    def __new__(cls, *args, **kwargs) -> TotalBidMatrixCalculationInputApply:
        warnings.warn(
            "TotalBidMatrixCalculationInputApply is deprecated and will be removed in v1.0. Use TotalBidMatrixCalculationInputWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "TotalBidMatrixCalculationInput.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)

class TotalBidMatrixCalculationInputList(DomainModelList[TotalBidMatrixCalculationInput]):
    """List of total bid matrix calculation inputs in the read version."""

    _INSTANCE = TotalBidMatrixCalculationInput
    def as_write(self) -> TotalBidMatrixCalculationInputWriteList:
        """Convert these read versions of total bid matrix calculation input to the writing versions."""
        return TotalBidMatrixCalculationInputWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> TotalBidMatrixCalculationInputWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def bid_configuration(self) -> BidConfigurationDayAheadList:
        from ._bid_configuration_day_ahead import BidConfigurationDayAhead, BidConfigurationDayAheadList
        return BidConfigurationDayAheadList([item.bid_configuration for item in self.data if isinstance(item.bid_configuration, BidConfigurationDayAhead)])
    @property
    def partial_bid_matrices(self) -> BidMatrixList:
        from ._bid_matrix import BidMatrix, BidMatrixList
        return BidMatrixList([item for items in self.data for item in items.partial_bid_matrices or [] if isinstance(item, BidMatrix)])


class TotalBidMatrixCalculationInputWriteList(DomainModelWriteList[TotalBidMatrixCalculationInputWrite]):
    """List of total bid matrix calculation inputs in the writing version."""

    _INSTANCE = TotalBidMatrixCalculationInputWrite
    @property
    def bid_configuration(self) -> BidConfigurationDayAheadWriteList:
        from ._bid_configuration_day_ahead import BidConfigurationDayAheadWrite, BidConfigurationDayAheadWriteList
        return BidConfigurationDayAheadWriteList([item.bid_configuration for item in self.data if isinstance(item.bid_configuration, BidConfigurationDayAheadWrite)])
    @property
    def partial_bid_matrices(self) -> BidMatrixWriteList:
        from ._bid_matrix import BidMatrixWrite, BidMatrixWriteList
        return BidMatrixWriteList([item for items in self.data for item in items.partial_bid_matrices or [] if isinstance(item, BidMatrixWrite)])


class TotalBidMatrixCalculationInputApplyList(TotalBidMatrixCalculationInputWriteList): ...


def _create_total_bid_matrix_calculation_input_filter(
    view_id: dm.ViewId,
    workflow_execution_id: str | list[str] | None = None,
    workflow_execution_id_prefix: str | None = None,
    min_workflow_step: int | None = None,
    max_workflow_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
    function_call_id: str | list[str] | None = None,
    function_call_id_prefix: str | None = None,
    bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    min_bid_date: datetime.date | None = None,
    max_bid_date: datetime.date | None = None,
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
    if isinstance(bid_configuration, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(bid_configuration):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bidConfiguration"), value=as_instance_dict_id(bid_configuration)))
    if bid_configuration and isinstance(bid_configuration, Sequence) and not isinstance(bid_configuration, str) and not is_tuple_id(bid_configuration):
        filters.append(dm.filters.In(view_id.as_property_ref("bidConfiguration"), values=[as_instance_dict_id(item) for item in bid_configuration]))
    if min_bid_date is not None or max_bid_date is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("bidDate"), gte=min_bid_date.isoformat() if min_bid_date else None, lte=max_bid_date.isoformat() if max_bid_date else None))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _TotalBidMatrixCalculationInputQuery(NodeQueryCore[T_DomainModelList, TotalBidMatrixCalculationInputList]):
    _view_id = TotalBidMatrixCalculationInput._view_id
    _result_cls = TotalBidMatrixCalculationInput
    _result_list_cls_end = TotalBidMatrixCalculationInputList

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
        from ._bid_configuration_day_ahead import _BidConfigurationDayAheadQuery
        from ._bid_matrix import _BidMatrixQuery

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

        if _BidMatrixQuery not in created_types:
            self.partial_bid_matrices = _BidMatrixQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="partial_bid_matrices",
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.workflow_execution_id = StringFilter(self, self._view_id.as_property_ref("workflowExecutionId"))
        self.workflow_step = IntFilter(self, self._view_id.as_property_ref("workflowStep"))
        self.function_name = StringFilter(self, self._view_id.as_property_ref("functionName"))
        self.function_call_id = StringFilter(self, self._view_id.as_property_ref("functionCallId"))
        self.bid_date = DateFilter(self, self._view_id.as_property_ref("bidDate"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.workflow_execution_id,
            self.workflow_step,
            self.function_name,
            self.function_call_id,
            self.bid_date,
        ])

    def list_total_bid_matrix_calculation_input(self, limit: int = DEFAULT_QUERY_LIMIT) -> TotalBidMatrixCalculationInputList:
        return self._list(limit=limit)


class TotalBidMatrixCalculationInputQuery(_TotalBidMatrixCalculationInputQuery[TotalBidMatrixCalculationInputList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, TotalBidMatrixCalculationInputList)
