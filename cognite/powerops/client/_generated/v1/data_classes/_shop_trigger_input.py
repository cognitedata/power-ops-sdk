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
from cognite.powerops.client._generated.v1.data_classes._function_input import FunctionInput, FunctionInputWrite
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._shop_case import ShopCase, ShopCaseList, ShopCaseGraphQL, ShopCaseWrite, ShopCaseWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_preprocessor_input import ShopPreprocessorInput, ShopPreprocessorInputList, ShopPreprocessorInputGraphQL, ShopPreprocessorInputWrite, ShopPreprocessorInputWriteList


__all__ = [
    "ShopTriggerInput",
    "ShopTriggerInputWrite",
    "ShopTriggerInputApply",
    "ShopTriggerInputList",
    "ShopTriggerInputWriteList",
    "ShopTriggerInputApplyList",
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
        preprocessor_input: The preprocessor input to the shop run
        case: The SHOP case (with all details like model, scenario, and time series)
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopTriggerInput", "1")
    workflow_execution_id: Optional[str] = Field(None, alias="workflowExecutionId")
    workflow_step: Optional[int] = Field(None, alias="workflowStep")
    function_name: Optional[str] = Field(None, alias="functionName")
    function_call_id: Optional[str] = Field(None, alias="functionCallId")
    cog_shop_tag: Optional[str] = Field(None, alias="cogShopTag")
    preprocessor_input: Optional[ShopPreprocessorInputGraphQL] = Field(default=None, repr=False, alias="preprocessorInput")
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


    @field_validator("preprocessor_input", "case", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ShopTriggerInput:
        """Convert this GraphQL format of shop trigger input to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ShopTriggerInput(
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
            cog_shop_tag=self.cog_shop_tag,
            preprocessor_input=self.preprocessor_input.as_read()
if isinstance(self.preprocessor_input, GraphQLCore)
else self.preprocessor_input,
            case=self.case.as_read()
if isinstance(self.case, GraphQLCore)
else self.case,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopTriggerInputWrite:
        """Convert this GraphQL format of shop trigger input to the writing format."""
        return ShopTriggerInputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            workflow_execution_id=self.workflow_execution_id,
            workflow_step=self.workflow_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            cog_shop_tag=self.cog_shop_tag,
            preprocessor_input=self.preprocessor_input.as_write()
if isinstance(self.preprocessor_input, GraphQLCore)
else self.preprocessor_input,
            case=self.case.as_write()
if isinstance(self.case, GraphQLCore)
else self.case,
        )


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
        preprocessor_input: The preprocessor input to the shop run
        case: The SHOP case (with all details like model, scenario, and time series)
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopTriggerInput", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopTriggerInput")
    cog_shop_tag: Optional[str] = Field(None, alias="cogShopTag")
    preprocessor_input: Union[ShopPreprocessorInput, str, dm.NodeId, None] = Field(default=None, repr=False, alias="preprocessorInput")
    case: Union[ShopCase, str, dm.NodeId, None] = Field(default=None, repr=False)

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopTriggerInputWrite:
        """Convert this read version of shop trigger input to the writing version."""
        return ShopTriggerInputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            workflow_execution_id=self.workflow_execution_id,
            workflow_step=self.workflow_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            cog_shop_tag=self.cog_shop_tag,
            preprocessor_input=self.preprocessor_input.as_write()
if isinstance(self.preprocessor_input, DomainModel)
else self.preprocessor_input,
            case=self.case.as_write()
if isinstance(self.case, DomainModel)
else self.case,
        )

    def as_apply(self) -> ShopTriggerInputWrite:
        """Convert this read version of shop trigger input to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()
    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, ShopTriggerInput],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._shop_case import ShopCase
        from ._shop_preprocessor_input import ShopPreprocessorInput
        for instance in instances.values():
            if isinstance(instance.preprocessor_input, (dm.NodeId, str)) and (preprocessor_input := nodes_by_id.get(instance.preprocessor_input)) and isinstance(
                    preprocessor_input, ShopPreprocessorInput
            ):
                instance.preprocessor_input = preprocessor_input
            if isinstance(instance.case, (dm.NodeId, str)) and (case := nodes_by_id.get(instance.case)) and isinstance(
                    case, ShopCase
            ):
                instance.case = case


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
        preprocessor_input: The preprocessor input to the shop run
        case: The SHOP case (with all details like model, scenario, and time series)
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopTriggerInput", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "ShopTriggerInput")
    cog_shop_tag: Optional[str] = Field(None, alias="cogShopTag")
    preprocessor_input: Union[ShopPreprocessorInputWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="preprocessorInput")
    case: Union[ShopCaseWrite, str, dm.NodeId, None] = Field(default=None, repr=False)

    @field_validator("preprocessor_input", "case", mode="before")
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

        if self.cog_shop_tag is not None or write_none:
            properties["cogShopTag"] = self.cog_shop_tag

        if self.preprocessor_input is not None:
            properties["preprocessorInput"] = {
                "space":  self.space if isinstance(self.preprocessor_input, str) else self.preprocessor_input.space,
                "externalId": self.preprocessor_input if isinstance(self.preprocessor_input, str) else self.preprocessor_input.external_id,
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

        if isinstance(self.preprocessor_input, DomainModelWrite):
            other_resources = self.preprocessor_input._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.case, DomainModelWrite):
            other_resources = self.case._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class ShopTriggerInputApply(ShopTriggerInputWrite):
    def __new__(cls, *args, **kwargs) -> ShopTriggerInputApply:
        warnings.warn(
            "ShopTriggerInputApply is deprecated and will be removed in v1.0. Use ShopTriggerInputWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ShopTriggerInput.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)

class ShopTriggerInputList(DomainModelList[ShopTriggerInput]):
    """List of shop trigger inputs in the read version."""

    _INSTANCE = ShopTriggerInput
    def as_write(self) -> ShopTriggerInputWriteList:
        """Convert these read versions of shop trigger input to the writing versions."""
        return ShopTriggerInputWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ShopTriggerInputWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def preprocessor_input(self) -> ShopPreprocessorInputList:
        from ._shop_preprocessor_input import ShopPreprocessorInput, ShopPreprocessorInputList
        return ShopPreprocessorInputList([item.preprocessor_input for item in self.data if isinstance(item.preprocessor_input, ShopPreprocessorInput)])
    @property
    def case(self) -> ShopCaseList:
        from ._shop_case import ShopCase, ShopCaseList
        return ShopCaseList([item.case for item in self.data if isinstance(item.case, ShopCase)])

class ShopTriggerInputWriteList(DomainModelWriteList[ShopTriggerInputWrite]):
    """List of shop trigger inputs in the writing version."""

    _INSTANCE = ShopTriggerInputWrite
    @property
    def preprocessor_input(self) -> ShopPreprocessorInputWriteList:
        from ._shop_preprocessor_input import ShopPreprocessorInputWrite, ShopPreprocessorInputWriteList
        return ShopPreprocessorInputWriteList([item.preprocessor_input for item in self.data if isinstance(item.preprocessor_input, ShopPreprocessorInputWrite)])
    @property
    def case(self) -> ShopCaseWriteList:
        from ._shop_case import ShopCaseWrite, ShopCaseWriteList
        return ShopCaseWriteList([item.case for item in self.data if isinstance(item.case, ShopCaseWrite)])

class ShopTriggerInputApplyList(ShopTriggerInputWriteList): ...


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
    preprocessor_input: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
    if isinstance(cog_shop_tag, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("cogShopTag"), value=cog_shop_tag))
    if cog_shop_tag and isinstance(cog_shop_tag, list):
        filters.append(dm.filters.In(view_id.as_property_ref("cogShopTag"), values=cog_shop_tag))
    if cog_shop_tag_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("cogShopTag"), value=cog_shop_tag_prefix))
    if isinstance(preprocessor_input, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(preprocessor_input):
        filters.append(dm.filters.Equals(view_id.as_property_ref("preprocessorInput"), value=as_instance_dict_id(preprocessor_input)))
    if preprocessor_input and isinstance(preprocessor_input, Sequence) and not isinstance(preprocessor_input, str) and not is_tuple_id(preprocessor_input):
        filters.append(dm.filters.In(view_id.as_property_ref("preprocessorInput"), values=[as_instance_dict_id(item) for item in preprocessor_input]))
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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
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
            connection_type,
            reverse_expression,
        )

        if _ShopPreprocessorInputQuery not in created_types:
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
        self.cog_shop_tag = StringFilter(self, self._view_id.as_property_ref("cogShopTag"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.workflow_execution_id,
            self.workflow_step,
            self.function_name,
            self.function_call_id,
            self.cog_shop_tag,
        ])

    def list_shop_trigger_input(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopTriggerInputList:
        return self._list(limit=limit)


class ShopTriggerInputQuery(_ShopTriggerInputQuery[ShopTriggerInputList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopTriggerInputList)
