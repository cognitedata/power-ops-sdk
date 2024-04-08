from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
)
from ._function_input import FunctionInput, FunctionInputWrite

if TYPE_CHECKING:
    from ._case import Case, CaseGraphQL, CaseWrite
    from ._preprocessor_input import PreprocessorInput, PreprocessorInputGraphQL, PreprocessorInputWrite


__all__ = [
    "SHOPTriggerInput",
    "SHOPTriggerInputWrite",
    "SHOPTriggerInputApply",
    "SHOPTriggerInputList",
    "SHOPTriggerInputWriteList",
    "SHOPTriggerInputApplyList",
    "SHOPTriggerInputFields",
    "SHOPTriggerInputTextFields",
]


SHOPTriggerInputTextFields = Literal["process_id", "function_name", "function_call_id", "cog_shop_tag"]
SHOPTriggerInputFields = Literal["process_id", "process_step", "function_name", "function_call_id", "cog_shop_tag"]

_SHOPTRIGGERINPUT_PROPERTIES_BY_FIELD = {
    "process_id": "processId",
    "process_step": "processStep",
    "function_name": "functionName",
    "function_call_id": "functionCallId",
    "cog_shop_tag": "cogShopTag",
}


class SHOPTriggerInputGraphQL(GraphQLCore):
    """This represents the reading version of shop trigger input, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop trigger input.
        data_record: The data record of the shop trigger input node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        cog_shop_tag: Optionally specify cogshop tag to trigger
        case: The scenario that is used in the shop run
        pre_processor_input: The preprocessor input to the shop run
    """

    view_id = dm.ViewId("sp_powerops_models_temp", "SHOPTriggerInput", "1")
    process_id: Optional[str] = Field(None, alias="processId")
    process_step: Optional[int] = Field(None, alias="processStep")
    function_name: Optional[str] = Field(None, alias="functionName")
    function_call_id: Optional[str] = Field(None, alias="functionCallId")
    cog_shop_tag: Optional[str] = Field(None, alias="cogShopTag")
    case: Optional[CaseGraphQL] = Field(None, repr=False)
    pre_processor_input: Optional[PreprocessorInputGraphQL] = Field(None, repr=False, alias="preProcessorInput")

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

    @field_validator("case", "pre_processor_input", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> SHOPTriggerInput:
        """Convert this GraphQL format of shop trigger input to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return SHOPTriggerInput(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            process_id=self.process_id,
            process_step=self.process_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            cog_shop_tag=self.cog_shop_tag,
            case=self.case.as_read() if isinstance(self.case, GraphQLCore) else self.case,
            pre_processor_input=(
                self.pre_processor_input.as_read()
                if isinstance(self.pre_processor_input, GraphQLCore)
                else self.pre_processor_input
            ),
        )

    def as_write(self) -> SHOPTriggerInputWrite:
        """Convert this GraphQL format of shop trigger input to the writing format."""
        return SHOPTriggerInputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            process_id=self.process_id,
            process_step=self.process_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            cog_shop_tag=self.cog_shop_tag,
            case=self.case.as_write() if isinstance(self.case, DomainModel) else self.case,
            pre_processor_input=(
                self.pre_processor_input.as_write()
                if isinstance(self.pre_processor_input, DomainModel)
                else self.pre_processor_input
            ),
        )


class SHOPTriggerInput(FunctionInput):
    """This represents the reading version of shop trigger input.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop trigger input.
        data_record: The data record of the shop trigger input node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        cog_shop_tag: Optionally specify cogshop tag to trigger
        case: The scenario that is used in the shop run
        pre_processor_input: The preprocessor input to the shop run
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types_temp", "SHOPTriggerInput"
    )
    cog_shop_tag: Optional[str] = Field(None, alias="cogShopTag")
    case: Union[Case, str, dm.NodeId, None] = Field(None, repr=False)
    pre_processor_input: Union[PreprocessorInput, str, dm.NodeId, None] = Field(
        None, repr=False, alias="preProcessorInput"
    )

    def as_write(self) -> SHOPTriggerInputWrite:
        """Convert this read version of shop trigger input to the writing version."""
        return SHOPTriggerInputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            process_id=self.process_id,
            process_step=self.process_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            cog_shop_tag=self.cog_shop_tag,
            case=self.case.as_write() if isinstance(self.case, DomainModel) else self.case,
            pre_processor_input=(
                self.pre_processor_input.as_write()
                if isinstance(self.pre_processor_input, DomainModel)
                else self.pre_processor_input
            ),
        )

    def as_apply(self) -> SHOPTriggerInputWrite:
        """Convert this read version of shop trigger input to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class SHOPTriggerInputWrite(FunctionInputWrite):
    """This represents the writing version of shop trigger input.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop trigger input.
        data_record: The data record of the shop trigger input node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        cog_shop_tag: Optionally specify cogshop tag to trigger
        case: The scenario that is used in the shop run
        pre_processor_input: The preprocessor input to the shop run
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types_temp", "SHOPTriggerInput"
    )
    cog_shop_tag: Optional[str] = Field(None, alias="cogShopTag")
    case: Union[CaseWrite, str, dm.NodeId, None] = Field(None, repr=False)
    pre_processor_input: Union[PreprocessorInputWrite, str, dm.NodeId, None] = Field(
        None, repr=False, alias="preProcessorInput"
    )

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            SHOPTriggerInput, dm.ViewId("sp_powerops_models_temp", "SHOPTriggerInput", "1")
        )

        properties: dict[str, Any] = {}

        if self.process_id is not None:
            properties["processId"] = self.process_id

        if self.process_step is not None:
            properties["processStep"] = self.process_step

        if self.function_name is not None:
            properties["functionName"] = self.function_name

        if self.function_call_id is not None:
            properties["functionCallId"] = self.function_call_id

        if self.cog_shop_tag is not None or write_none:
            properties["cogShopTag"] = self.cog_shop_tag

        if self.case is not None:
            properties["case"] = {
                "space": self.space if isinstance(self.case, str) else self.case.space,
                "externalId": self.case if isinstance(self.case, str) else self.case.external_id,
            }

        if self.pre_processor_input is not None:
            properties["preProcessorInput"] = {
                "space": self.space if isinstance(self.pre_processor_input, str) else self.pre_processor_input.space,
                "externalId": (
                    self.pre_processor_input
                    if isinstance(self.pre_processor_input, str)
                    else self.pre_processor_input.external_id
                ),
            }

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.case, DomainModelWrite):
            other_resources = self.case._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.pre_processor_input, DomainModelWrite):
            other_resources = self.pre_processor_input._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class SHOPTriggerInputApply(SHOPTriggerInputWrite):
    def __new__(cls, *args, **kwargs) -> SHOPTriggerInputApply:
        warnings.warn(
            "SHOPTriggerInputApply is deprecated and will be removed in v1.0. Use SHOPTriggerInputWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "SHOPTriggerInput.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class SHOPTriggerInputList(DomainModelList[SHOPTriggerInput]):
    """List of shop trigger inputs in the read version."""

    _INSTANCE = SHOPTriggerInput

    def as_write(self) -> SHOPTriggerInputWriteList:
        """Convert these read versions of shop trigger input to the writing versions."""
        return SHOPTriggerInputWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> SHOPTriggerInputWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class SHOPTriggerInputWriteList(DomainModelWriteList[SHOPTriggerInputWrite]):
    """List of shop trigger inputs in the writing version."""

    _INSTANCE = SHOPTriggerInputWrite


class SHOPTriggerInputApplyList(SHOPTriggerInputWriteList): ...


def _create_shop_trigger_input_filter(
    view_id: dm.ViewId,
    process_id: str | list[str] | None = None,
    process_id_prefix: str | None = None,
    min_process_step: int | None = None,
    max_process_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
    function_call_id: str | list[str] | None = None,
    function_call_id_prefix: str | None = None,
    cog_shop_tag: str | list[str] | None = None,
    cog_shop_tag_prefix: str | None = None,
    case: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    pre_processor_input: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if isinstance(process_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("processId"), value=process_id))
    if process_id and isinstance(process_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("processId"), values=process_id))
    if process_id_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("processId"), value=process_id_prefix))
    if min_process_step is not None or max_process_step is not None:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("processStep"), gte=min_process_step, lte=max_process_step)
        )
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
    if case and isinstance(case, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("case"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": case}
            )
        )
    if case and isinstance(case, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("case"), value={"space": case[0], "externalId": case[1]})
        )
    if case and isinstance(case, list) and isinstance(case[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("case"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in case],
            )
        )
    if case and isinstance(case, list) and isinstance(case[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("case"), values=[{"space": item[0], "externalId": item[1]} for item in case]
            )
        )
    if pre_processor_input and isinstance(pre_processor_input, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("preProcessorInput"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": pre_processor_input},
            )
        )
    if pre_processor_input and isinstance(pre_processor_input, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("preProcessorInput"),
                value={"space": pre_processor_input[0], "externalId": pre_processor_input[1]},
            )
        )
    if pre_processor_input and isinstance(pre_processor_input, list) and isinstance(pre_processor_input[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("preProcessorInput"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in pre_processor_input],
            )
        )
    if pre_processor_input and isinstance(pre_processor_input, list) and isinstance(pre_processor_input[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("preProcessorInput"),
                values=[{"space": item[0], "externalId": item[1]} for item in pre_processor_input],
            )
        )
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
