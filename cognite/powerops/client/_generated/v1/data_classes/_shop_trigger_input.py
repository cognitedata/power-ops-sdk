from __future__ import annotations

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
    IntFilter,
)
from cognite.powerops.client._generated.v1.data_classes._function_input import FunctionInput, FunctionInputWrite
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._shop_case import ShopCase, ShopCaseList, ShopCaseGraphQL, ShopCaseWrite, ShopCaseWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_preprocessor_input import ShopPreprocessorInput, ShopPreprocessorInputList, ShopPreprocessorInputGraphQL, ShopPreprocessorInputWrite, ShopPreprocessorInputWriteList


__all__ = [
    "ShopTriggerInput",
    "ShopTriggerInputWrite",
    "ShopTriggerInputList",
    "ShopTriggerInputWriteList",
    "ShopTriggerInputFields",
    "ShopTriggerInputTextFields",
    "ShopTriggerInputGraphQL",
]


ShopTriggerInputTextFields = Literal["external_id", "workflow_execution_id", "function_name", "function_call_id", "cog_shop_tag"]
ShopTriggerInputFields = Literal["external_id", "workflow_execution_id", "workflow_step", "function_name", "function_call_id", "cog_shop_tag"]

_SHOPTRIGGERINPUT_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "workflow_execution_id": "workflowExecutionId",
    "workflow_step": "workflowStep",
    "function_name": "functionName",
    "function_call_id": "functionCallId",
    "cog_shop_tag": "cogShopTag",
}


class ShopTriggerInputGraphQL(GraphQLCore):
    """This represents the reading version of shop trigger input, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop trigger input.
        data_record: The data record of the shop trigger input node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        cog_shop_tag: Optionally specify cogshop tag to trigger
        case: The SHOP case (with all details like model, scenario, and time series)
        preprocessor_input: The preprocessor input to the shop run
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopTriggerInput", "1")
    workflow_execution_id: Optional[str] = Field(None, alias="workflowExecutionId")
    workflow_step: Optional[int] = Field(None, alias="workflowStep")
    function_name: Optional[str] = Field(None, alias="functionName")
    function_call_id: Optional[str] = Field(None, alias="functionCallId")
    cog_shop_tag: Optional[str] = Field(None, alias="cogShopTag")
    case: Optional[ShopCaseGraphQL] = Field(default=None, repr=False)
    preprocessor_input: Optional[ShopPreprocessorInputGraphQL] = Field(default=None, repr=False, alias="preprocessorInput")

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


    @field_validator("case", "preprocessor_input", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> ShopTriggerInput:
        """Convert this GraphQL format of shop trigger input to the reading format."""
        return ShopTriggerInput.model_validate(as_read_args(self))

    def as_write(self) -> ShopTriggerInputWrite:
        """Convert this GraphQL format of shop trigger input to the writing format."""
        return ShopTriggerInputWrite.model_validate(as_write_args(self))


class ShopTriggerInput(FunctionInput):
    """This represents the reading version of shop trigger input.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop trigger input.
        data_record: The data record of the shop trigger input node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        cog_shop_tag: Optionally specify cogshop tag to trigger
        case: The SHOP case (with all details like model, scenario, and time series)
        preprocessor_input: The preprocessor input to the shop run
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopTriggerInput", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopTriggerInput")
    cog_shop_tag: Optional[str] = Field(None, alias="cogShopTag")
    case: Union[ShopCase, str, dm.NodeId, None] = Field(default=None, repr=False)
    preprocessor_input: Union[ShopPreprocessorInput, str, dm.NodeId, None] = Field(default=None, repr=False, alias="preprocessorInput")
    @field_validator("case", "preprocessor_input", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)


    def as_write(self) -> ShopTriggerInputWrite:
        """Convert this read version of shop trigger input to the writing version."""
        return ShopTriggerInputWrite.model_validate(as_write_args(self))



class ShopTriggerInputWrite(FunctionInputWrite):
    """This represents the writing version of shop trigger input.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop trigger input.
        data_record: The data record of the shop trigger input node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        cog_shop_tag: Optionally specify cogshop tag to trigger
        case: The SHOP case (with all details like model, scenario, and time series)
        preprocessor_input: The preprocessor input to the shop run
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("case", "cog_shop_tag", "function_call_id", "function_name", "preprocessor_input", "workflow_execution_id", "workflow_step",)
    _direct_relations: ClassVar[tuple[str, ...]] = ("case", "preprocessor_input",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopTriggerInput", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "ShopTriggerInput")
    cog_shop_tag: Optional[str] = Field(None, alias="cogShopTag")
    case: Union[ShopCaseWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    preprocessor_input: Union[ShopPreprocessorInputWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="preprocessorInput")

    @field_validator("case", "preprocessor_input", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class ShopTriggerInputList(DomainModelList[ShopTriggerInput]):
    """List of shop trigger inputs in the read version."""

    _INSTANCE = ShopTriggerInput
    def as_write(self) -> ShopTriggerInputWriteList:
        """Convert these read versions of shop trigger input to the writing versions."""
        return ShopTriggerInputWriteList([node.as_write() for node in self.data])


    @property
    def case(self) -> ShopCaseList:
        from ._shop_case import ShopCase, ShopCaseList
        return ShopCaseList([item.case for item in self.data if isinstance(item.case, ShopCase)])
    @property
    def preprocessor_input(self) -> ShopPreprocessorInputList:
        from ._shop_preprocessor_input import ShopPreprocessorInput, ShopPreprocessorInputList
        return ShopPreprocessorInputList([item.preprocessor_input for item in self.data if isinstance(item.preprocessor_input, ShopPreprocessorInput)])

class ShopTriggerInputWriteList(DomainModelWriteList[ShopTriggerInputWrite]):
    """List of shop trigger inputs in the writing version."""

    _INSTANCE = ShopTriggerInputWrite
    @property
    def case(self) -> ShopCaseWriteList:
        from ._shop_case import ShopCaseWrite, ShopCaseWriteList
        return ShopCaseWriteList([item.case for item in self.data if isinstance(item.case, ShopCaseWrite)])
    @property
    def preprocessor_input(self) -> ShopPreprocessorInputWriteList:
        from ._shop_preprocessor_input import ShopPreprocessorInputWrite, ShopPreprocessorInputWriteList
        return ShopPreprocessorInputWriteList([item.preprocessor_input for item in self.data if isinstance(item.preprocessor_input, ShopPreprocessorInputWrite)])


def _create_shop_trigger_input_filter(
    view_id: dm.ViewId,
    workflow_execution_id: str | list[str] | None = None,
    workflow_execution_id_prefix: str | None = None,
    min_workflow_step: int | None = None,
    max_workflow_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
    function_call_id: str | list[str] | None = None,
    function_call_id_prefix: str | None = None,
    cog_shop_tag: str | list[str] | None = None,
    cog_shop_tag_prefix: str | None = None,
    case: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    preprocessor_input: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
    if isinstance(cog_shop_tag, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("cogShopTag"), value=cog_shop_tag))
    if cog_shop_tag and isinstance(cog_shop_tag, list):
        filters.append(dm.filters.In(view_id.as_property_ref("cogShopTag"), values=cog_shop_tag))
    if cog_shop_tag_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("cogShopTag"), value=cog_shop_tag_prefix))
    if isinstance(case, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(case):
        filters.append(dm.filters.Equals(view_id.as_property_ref("case"), value=as_instance_dict_id(case)))
    if case and isinstance(case, Sequence) and not isinstance(case, str) and not is_tuple_id(case):
        filters.append(dm.filters.In(view_id.as_property_ref("case"), values=[as_instance_dict_id(item) for item in case]))
    if isinstance(preprocessor_input, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(preprocessor_input):
        filters.append(dm.filters.Equals(view_id.as_property_ref("preprocessorInput"), value=as_instance_dict_id(preprocessor_input)))
    if preprocessor_input and isinstance(preprocessor_input, Sequence) and not isinstance(preprocessor_input, str) and not is_tuple_id(preprocessor_input):
        filters.append(dm.filters.In(view_id.as_property_ref("preprocessorInput"), values=[as_instance_dict_id(item) for item in preprocessor_input]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _ShopTriggerInputQuery(NodeQueryCore[T_DomainModelList, ShopTriggerInputList]):
    _view_id = ShopTriggerInput._view_id
    _result_cls = ShopTriggerInput
    _result_list_cls_end = ShopTriggerInputList

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

        if _ShopPreprocessorInputQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.preprocessor_input = _ShopPreprocessorInputQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("preprocessorInput"),
                    direction="outwards",
                ),
                connection_name="preprocessor_input",
                connection_property=ViewPropertyId(self._view_id, "preprocessorInput"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.workflow_execution_id = StringFilter(self, self._view_id.as_property_ref("workflowExecutionId"))
        self.workflow_step = IntFilter(self, self._view_id.as_property_ref("workflowStep"))
        self.function_name = StringFilter(self, self._view_id.as_property_ref("functionName"))
        self.function_call_id = StringFilter(self, self._view_id.as_property_ref("functionCallId"))
        self.cog_shop_tag = StringFilter(self, self._view_id.as_property_ref("cogShopTag"))
        self.case_filter = DirectRelationFilter(self, self._view_id.as_property_ref("case"))
        self.preprocessor_input_filter = DirectRelationFilter(self, self._view_id.as_property_ref("preprocessorInput"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.workflow_execution_id,
            self.workflow_step,
            self.function_name,
            self.function_call_id,
            self.cog_shop_tag,
            self.case_filter,
            self.preprocessor_input_filter,
        ])

    def list_shop_trigger_input(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopTriggerInputList:
        return self._list(limit=limit)


class ShopTriggerInputQuery(_ShopTriggerInputQuery[ShopTriggerInputList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopTriggerInputList)
