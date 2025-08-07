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
    DateFilter,
    DirectRelationFilter,
    IntFilter,
)
from cognite.powerops.client._generated.v1.data_classes._function_input import FunctionInput, FunctionInputWrite
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._bid_configuration_day_ahead import BidConfigurationDayAhead, BidConfigurationDayAheadList, BidConfigurationDayAheadGraphQL, BidConfigurationDayAheadWrite, BidConfigurationDayAheadWriteList
    from cognite.powerops.client._generated.v1.data_classes._bid_matrix import BidMatrix, BidMatrixList, BidMatrixGraphQL, BidMatrixWrite, BidMatrixWriteList


__all__ = [
    "TotalBidMatrixCalculationInput",
    "TotalBidMatrixCalculationInputWrite",
    "TotalBidMatrixCalculationInputList",
    "TotalBidMatrixCalculationInputWriteList",
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

    def as_read(self) -> TotalBidMatrixCalculationInput:
        """Convert this GraphQL format of total bid matrix calculation input to the reading format."""
        return TotalBidMatrixCalculationInput.model_validate(as_read_args(self))

    def as_write(self) -> TotalBidMatrixCalculationInputWrite:
        """Convert this GraphQL format of total bid matrix calculation input to the writing format."""
        return TotalBidMatrixCalculationInputWrite.model_validate(as_write_args(self))


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
    @field_validator("bid_configuration", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("partial_bid_matrices", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> TotalBidMatrixCalculationInputWrite:
        """Convert this read version of total bid matrix calculation input to the writing version."""
        return TotalBidMatrixCalculationInputWrite.model_validate(as_write_args(self))



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
    _container_fields: ClassVar[tuple[str, ...]] = ("bid_configuration", "bid_date", "function_call_id", "function_name", "workflow_execution_id", "workflow_step",)
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (("partial_bid_matrices", dm.DirectRelationReference("power_ops_types", "BidMatrix")),)
    _direct_relations: ClassVar[tuple[str, ...]] = ("bid_configuration",)

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


class TotalBidMatrixCalculationInputList(DomainModelList[TotalBidMatrixCalculationInput]):
    """List of total bid matrix calculation inputs in the read version."""

    _INSTANCE = TotalBidMatrixCalculationInput
    def as_write(self) -> TotalBidMatrixCalculationInputWriteList:
        """Convert these read versions of total bid matrix calculation input to the writing versions."""
        return TotalBidMatrixCalculationInputWriteList([node.as_write() for node in self.data])


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
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
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
            connection_property,
            connection_type,
            reverse_expression,
        )

        if _BidConfigurationDayAheadQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "bidConfiguration"),
            )

        if _BidMatrixQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "partialBidMatrices"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.workflow_execution_id = StringFilter(self, self._view_id.as_property_ref("workflowExecutionId"))
        self.workflow_step = IntFilter(self, self._view_id.as_property_ref("workflowStep"))
        self.function_name = StringFilter(self, self._view_id.as_property_ref("functionName"))
        self.function_call_id = StringFilter(self, self._view_id.as_property_ref("functionCallId"))
        self.bid_configuration_filter = DirectRelationFilter(self, self._view_id.as_property_ref("bidConfiguration"))
        self.bid_date = DateFilter(self, self._view_id.as_property_ref("bidDate"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.workflow_execution_id,
            self.workflow_step,
            self.function_name,
            self.function_call_id,
            self.bid_configuration_filter,
            self.bid_date,
        ])

    def list_total_bid_matrix_calculation_input(self, limit: int = DEFAULT_QUERY_LIMIT) -> TotalBidMatrixCalculationInputList:
        return self._list(limit=limit)


class TotalBidMatrixCalculationInputQuery(_TotalBidMatrixCalculationInputQuery[TotalBidMatrixCalculationInputList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, TotalBidMatrixCalculationInputList)
