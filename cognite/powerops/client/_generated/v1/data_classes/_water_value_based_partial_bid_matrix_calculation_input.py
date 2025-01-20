from __future__ import annotations

import datetime
import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

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
from cognite.powerops.client._generated.v1.data_classes._partial_bid_matrix_calculation_input import PartialBidMatrixCalculationInput, PartialBidMatrixCalculationInputWrite
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._bid_configuration_day_ahead import BidConfigurationDayAhead, BidConfigurationDayAheadList, BidConfigurationDayAheadGraphQL, BidConfigurationDayAheadWrite, BidConfigurationDayAheadWriteList
    from cognite.powerops.client._generated.v1.data_classes._water_value_based_partial_bid_configuration import WaterValueBasedPartialBidConfiguration, WaterValueBasedPartialBidConfigurationList, WaterValueBasedPartialBidConfigurationGraphQL, WaterValueBasedPartialBidConfigurationWrite, WaterValueBasedPartialBidConfigurationWriteList


__all__ = [
    "WaterValueBasedPartialBidMatrixCalculationInput",
    "WaterValueBasedPartialBidMatrixCalculationInputWrite",
    "WaterValueBasedPartialBidMatrixCalculationInputApply",
    "WaterValueBasedPartialBidMatrixCalculationInputList",
    "WaterValueBasedPartialBidMatrixCalculationInputWriteList",
    "WaterValueBasedPartialBidMatrixCalculationInputApplyList",
    "WaterValueBasedPartialBidMatrixCalculationInputFields",
    "WaterValueBasedPartialBidMatrixCalculationInputTextFields",
    "WaterValueBasedPartialBidMatrixCalculationInputGraphQL",
]


WaterValueBasedPartialBidMatrixCalculationInputTextFields = Literal["external_id", "workflow_execution_id", "function_name", "function_call_id"]
WaterValueBasedPartialBidMatrixCalculationInputFields = Literal["external_id", "workflow_execution_id", "workflow_step", "function_name", "function_call_id", "bid_date"]

_WATERVALUEBASEDPARTIALBIDMATRIXCALCULATIONINPUT_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "workflow_execution_id": "workflowExecutionId",
    "workflow_step": "workflowStep",
    "function_name": "functionName",
    "function_call_id": "functionCallId",
    "bid_date": "bidDate",
}


class WaterValueBasedPartialBidMatrixCalculationInputGraphQL(GraphQLCore):
    """This represents the reading version of water value based partial bid matrix calculation input, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water value based partial bid matrix calculation input.
        data_record: The data record of the water value based partial bid matrix calculation input node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        bid_date: The bid date
        bid_configuration: TODO description
        partial_bid_configuration: The partial bid configuration related to the bid calculation task
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "WaterValueBasedPartialBidMatrixCalculationInput", "1")
    workflow_execution_id: Optional[str] = Field(None, alias="workflowExecutionId")
    workflow_step: Optional[int] = Field(None, alias="workflowStep")
    function_name: Optional[str] = Field(None, alias="functionName")
    function_call_id: Optional[str] = Field(None, alias="functionCallId")
    bid_date: Optional[datetime.date] = Field(None, alias="bidDate")
    bid_configuration: Optional[BidConfigurationDayAheadGraphQL] = Field(default=None, repr=False, alias="bidConfiguration")
    partial_bid_configuration: Optional[WaterValueBasedPartialBidConfigurationGraphQL] = Field(default=None, repr=False, alias="partialBidConfiguration")

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


    @field_validator("bid_configuration", "partial_bid_configuration", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> WaterValueBasedPartialBidMatrixCalculationInput:
        """Convert this GraphQL format of water value based partial bid matrix calculation input to the reading format."""
        return WaterValueBasedPartialBidMatrixCalculationInput.model_validate(as_read_args(self))

    def as_write(self) -> WaterValueBasedPartialBidMatrixCalculationInputWrite:
        """Convert this GraphQL format of water value based partial bid matrix calculation input to the writing format."""
        return WaterValueBasedPartialBidMatrixCalculationInputWrite.model_validate(as_write_args(self))


class WaterValueBasedPartialBidMatrixCalculationInput(PartialBidMatrixCalculationInput):
    """This represents the reading version of water value based partial bid matrix calculation input.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water value based partial bid matrix calculation input.
        data_record: The data record of the water value based partial bid matrix calculation input node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        bid_date: The bid date
        bid_configuration: TODO description
        partial_bid_configuration: The partial bid configuration related to the bid calculation task
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "WaterValueBasedPartialBidMatrixCalculationInput", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "WaterValueBasedPartialBidMatrixCalculationInput")
    @field_validator("bid_configuration", "partial_bid_configuration", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)


    def as_write(self) -> WaterValueBasedPartialBidMatrixCalculationInputWrite:
        """Convert this read version of water value based partial bid matrix calculation input to the writing version."""
        return WaterValueBasedPartialBidMatrixCalculationInputWrite.model_validate(as_write_args(self))

    def as_apply(self) -> WaterValueBasedPartialBidMatrixCalculationInputWrite:
        """Convert this read version of water value based partial bid matrix calculation input to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class WaterValueBasedPartialBidMatrixCalculationInputWrite(PartialBidMatrixCalculationInputWrite):
    """This represents the writing version of water value based partial bid matrix calculation input.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water value based partial bid matrix calculation input.
        data_record: The data record of the water value based partial bid matrix calculation input node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        bid_date: The bid date
        bid_configuration: TODO description
        partial_bid_configuration: The partial bid configuration related to the bid calculation task
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("bid_configuration", "bid_date", "function_call_id", "function_name", "partial_bid_configuration", "workflow_execution_id", "workflow_step",)
    _direct_relations: ClassVar[tuple[str, ...]] = ("bid_configuration", "partial_bid_configuration",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "WaterValueBasedPartialBidMatrixCalculationInput", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "WaterValueBasedPartialBidMatrixCalculationInput")



class WaterValueBasedPartialBidMatrixCalculationInputApply(WaterValueBasedPartialBidMatrixCalculationInputWrite):
    def __new__(cls, *args, **kwargs) -> WaterValueBasedPartialBidMatrixCalculationInputApply:
        warnings.warn(
            "WaterValueBasedPartialBidMatrixCalculationInputApply is deprecated and will be removed in v1.0. "
            "Use WaterValueBasedPartialBidMatrixCalculationInputWrite instead. "
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "WaterValueBasedPartialBidMatrixCalculationInput.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)

class WaterValueBasedPartialBidMatrixCalculationInputList(DomainModelList[WaterValueBasedPartialBidMatrixCalculationInput]):
    """List of water value based partial bid matrix calculation inputs in the read version."""

    _INSTANCE = WaterValueBasedPartialBidMatrixCalculationInput
    def as_write(self) -> WaterValueBasedPartialBidMatrixCalculationInputWriteList:
        """Convert these read versions of water value based partial bid matrix calculation input to the writing versions."""
        return WaterValueBasedPartialBidMatrixCalculationInputWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> WaterValueBasedPartialBidMatrixCalculationInputWriteList:
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
    def partial_bid_configuration(self) -> WaterValueBasedPartialBidConfigurationList:
        from ._water_value_based_partial_bid_configuration import WaterValueBasedPartialBidConfiguration, WaterValueBasedPartialBidConfigurationList
        return WaterValueBasedPartialBidConfigurationList([item.partial_bid_configuration for item in self.data if isinstance(item.partial_bid_configuration, WaterValueBasedPartialBidConfiguration)])

class WaterValueBasedPartialBidMatrixCalculationInputWriteList(DomainModelWriteList[WaterValueBasedPartialBidMatrixCalculationInputWrite]):
    """List of water value based partial bid matrix calculation inputs in the writing version."""

    _INSTANCE = WaterValueBasedPartialBidMatrixCalculationInputWrite
    @property
    def bid_configuration(self) -> BidConfigurationDayAheadWriteList:
        from ._bid_configuration_day_ahead import BidConfigurationDayAheadWrite, BidConfigurationDayAheadWriteList
        return BidConfigurationDayAheadWriteList([item.bid_configuration for item in self.data if isinstance(item.bid_configuration, BidConfigurationDayAheadWrite)])
    @property
    def partial_bid_configuration(self) -> WaterValueBasedPartialBidConfigurationWriteList:
        from ._water_value_based_partial_bid_configuration import WaterValueBasedPartialBidConfigurationWrite, WaterValueBasedPartialBidConfigurationWriteList
        return WaterValueBasedPartialBidConfigurationWriteList([item.partial_bid_configuration for item in self.data if isinstance(item.partial_bid_configuration, WaterValueBasedPartialBidConfigurationWrite)])

class WaterValueBasedPartialBidMatrixCalculationInputApplyList(WaterValueBasedPartialBidMatrixCalculationInputWriteList): ...


def _create_water_value_based_partial_bid_matrix_calculation_input_filter(
    view_id: dm.ViewId,
    workflow_execution_id: str | list[str] | None = None,
    workflow_execution_id_prefix: str | None = None,
    min_workflow_step: int | None = None,
    max_workflow_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
    function_call_id: str | list[str] | None = None,
    function_call_id_prefix: str | None = None,
    min_bid_date: datetime.date | None = None,
    max_bid_date: datetime.date | None = None,
    bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    partial_bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
    if min_bid_date is not None or max_bid_date is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("bidDate"), gte=min_bid_date.isoformat() if min_bid_date else None, lte=max_bid_date.isoformat() if max_bid_date else None))
    if isinstance(bid_configuration, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(bid_configuration):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bidConfiguration"), value=as_instance_dict_id(bid_configuration)))
    if bid_configuration and isinstance(bid_configuration, Sequence) and not isinstance(bid_configuration, str) and not is_tuple_id(bid_configuration):
        filters.append(dm.filters.In(view_id.as_property_ref("bidConfiguration"), values=[as_instance_dict_id(item) for item in bid_configuration]))
    if isinstance(partial_bid_configuration, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(partial_bid_configuration):
        filters.append(dm.filters.Equals(view_id.as_property_ref("partialBidConfiguration"), value=as_instance_dict_id(partial_bid_configuration)))
    if partial_bid_configuration and isinstance(partial_bid_configuration, Sequence) and not isinstance(partial_bid_configuration, str) and not is_tuple_id(partial_bid_configuration):
        filters.append(dm.filters.In(view_id.as_property_ref("partialBidConfiguration"), values=[as_instance_dict_id(item) for item in partial_bid_configuration]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _WaterValueBasedPartialBidMatrixCalculationInputQuery(NodeQueryCore[T_DomainModelList, WaterValueBasedPartialBidMatrixCalculationInputList]):
    _view_id = WaterValueBasedPartialBidMatrixCalculationInput._view_id
    _result_cls = WaterValueBasedPartialBidMatrixCalculationInput
    _result_list_cls_end = WaterValueBasedPartialBidMatrixCalculationInputList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
    ):
        from ._bid_configuration_day_ahead import _BidConfigurationDayAheadQuery
        from ._water_value_based_partial_bid_configuration import _WaterValueBasedPartialBidConfigurationQuery

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
                connection_property=ViewPropertyId(self._view_id, "bidConfiguration"),
            )

        if _WaterValueBasedPartialBidConfigurationQuery not in created_types:
            self.partial_bid_configuration = _WaterValueBasedPartialBidConfigurationQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("partialBidConfiguration"),
                    direction="outwards",
                ),
                connection_name="partial_bid_configuration",
                connection_property=ViewPropertyId(self._view_id, "partialBidConfiguration"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.workflow_execution_id = StringFilter(self, self._view_id.as_property_ref("workflowExecutionId"))
        self.workflow_step = IntFilter(self, self._view_id.as_property_ref("workflowStep"))
        self.function_name = StringFilter(self, self._view_id.as_property_ref("functionName"))
        self.function_call_id = StringFilter(self, self._view_id.as_property_ref("functionCallId"))
        self.bid_date = DateFilter(self, self._view_id.as_property_ref("bidDate"))
        self.bid_configuration_filter = DirectRelationFilter(self, self._view_id.as_property_ref("bidConfiguration"))
        self.partial_bid_configuration_filter = DirectRelationFilter(self, self._view_id.as_property_ref("partialBidConfiguration"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.workflow_execution_id,
            self.workflow_step,
            self.function_name,
            self.function_call_id,
            self.bid_date,
            self.bid_configuration_filter,
            self.partial_bid_configuration_filter,
        ])

    def list_water_value_based_partial_bid_matrix_calculation_input(self, limit: int = DEFAULT_QUERY_LIMIT) -> WaterValueBasedPartialBidMatrixCalculationInputList:
        return self._list(limit=limit)


class WaterValueBasedPartialBidMatrixCalculationInputQuery(_WaterValueBasedPartialBidMatrixCalculationInputQuery[WaterValueBasedPartialBidMatrixCalculationInputList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, WaterValueBasedPartialBidMatrixCalculationInputList)
