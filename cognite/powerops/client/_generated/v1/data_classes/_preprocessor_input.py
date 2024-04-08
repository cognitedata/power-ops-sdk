from __future__ import annotations

import datetime
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
    from ._scenario import Scenario, ScenarioGraphQL, ScenarioWrite


__all__ = [
    "PreprocessorInput",
    "PreprocessorInputWrite",
    "PreprocessorInputApply",
    "PreprocessorInputList",
    "PreprocessorInputWriteList",
    "PreprocessorInputApplyList",
    "PreprocessorInputFields",
    "PreprocessorInputTextFields",
]


PreprocessorInputTextFields = Literal["process_id", "function_name", "function_call_id"]
PreprocessorInputFields = Literal[
    "process_id", "process_step", "function_name", "function_call_id", "shop_start", "shop_end"
]

_PREPROCESSORINPUT_PROPERTIES_BY_FIELD = {
    "process_id": "processId",
    "process_step": "processStep",
    "function_name": "functionName",
    "function_call_id": "functionCallId",
    "shop_start": "shopStart",
    "shop_end": "shopEnd",
}


class PreprocessorInputGraphQL(GraphQLCore):
    """This represents the reading version of preprocessor input, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the preprocessor input.
        data_record: The data record of the preprocessor input node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        scenario: The scenario to run shop with
        shop_start: Start date of bid period
        shop_end: End date of bid period
    """

    view_id = dm.ViewId("sp_powerops_models_temp", "PreprocessorInput", "1")
    process_id: Optional[str] = Field(None, alias="processId")
    process_step: Optional[int] = Field(None, alias="processStep")
    function_name: Optional[str] = Field(None, alias="functionName")
    function_call_id: Optional[str] = Field(None, alias="functionCallId")
    scenario: Optional[ScenarioGraphQL] = Field(None, repr=False)
    shop_start: Optional[datetime.datetime] = Field(None, alias="shopStart")
    shop_end: Optional[datetime.datetime] = Field(None, alias="shopEnd")

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

    @field_validator("scenario", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> PreprocessorInput:
        """Convert this GraphQL format of preprocessor input to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return PreprocessorInput(
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
            scenario=self.scenario.as_read() if isinstance(self.scenario, GraphQLCore) else self.scenario,
            shop_start=self.shop_start,
            shop_end=self.shop_end,
        )

    def as_write(self) -> PreprocessorInputWrite:
        """Convert this GraphQL format of preprocessor input to the writing format."""
        return PreprocessorInputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            process_id=self.process_id,
            process_step=self.process_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            scenario=self.scenario.as_write() if isinstance(self.scenario, DomainModel) else self.scenario,
            shop_start=self.shop_start,
            shop_end=self.shop_end,
        )


class PreprocessorInput(FunctionInput):
    """This represents the reading version of preprocessor input.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the preprocessor input.
        data_record: The data record of the preprocessor input node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        scenario: The scenario to run shop with
        shop_start: Start date of bid period
        shop_end: End date of bid period
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types_temp", "PreprocessorInput"
    )
    scenario: Union[Scenario, str, dm.NodeId, None] = Field(None, repr=False)
    shop_start: Optional[datetime.datetime] = Field(None, alias="shopStart")
    shop_end: Optional[datetime.datetime] = Field(None, alias="shopEnd")

    def as_write(self) -> PreprocessorInputWrite:
        """Convert this read version of preprocessor input to the writing version."""
        return PreprocessorInputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            process_id=self.process_id,
            process_step=self.process_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            scenario=self.scenario.as_write() if isinstance(self.scenario, DomainModel) else self.scenario,
            shop_start=self.shop_start,
            shop_end=self.shop_end,
        )

    def as_apply(self) -> PreprocessorInputWrite:
        """Convert this read version of preprocessor input to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PreprocessorInputWrite(FunctionInputWrite):
    """This represents the writing version of preprocessor input.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the preprocessor input.
        data_record: The data record of the preprocessor input node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        scenario: The scenario to run shop with
        shop_start: Start date of bid period
        shop_end: End date of bid period
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types_temp", "PreprocessorInput"
    )
    scenario: Union[ScenarioWrite, str, dm.NodeId, None] = Field(None, repr=False)
    shop_start: Optional[datetime.datetime] = Field(None, alias="shopStart")
    shop_end: Optional[datetime.datetime] = Field(None, alias="shopEnd")

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
            PreprocessorInput, dm.ViewId("sp_powerops_models_temp", "PreprocessorInput", "1")
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

        if self.scenario is not None:
            properties["scenario"] = {
                "space": self.space if isinstance(self.scenario, str) else self.scenario.space,
                "externalId": self.scenario if isinstance(self.scenario, str) else self.scenario.external_id,
            }

        if self.shop_start is not None or write_none:
            properties["shopStart"] = self.shop_start.isoformat(timespec="milliseconds") if self.shop_start else None

        if self.shop_end is not None or write_none:
            properties["shopEnd"] = self.shop_end.isoformat(timespec="milliseconds") if self.shop_end else None

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

        if isinstance(self.scenario, DomainModelWrite):
            other_resources = self.scenario._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class PreprocessorInputApply(PreprocessorInputWrite):
    def __new__(cls, *args, **kwargs) -> PreprocessorInputApply:
        warnings.warn(
            "PreprocessorInputApply is deprecated and will be removed in v1.0. Use PreprocessorInputWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PreprocessorInput.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class PreprocessorInputList(DomainModelList[PreprocessorInput]):
    """List of preprocessor inputs in the read version."""

    _INSTANCE = PreprocessorInput

    def as_write(self) -> PreprocessorInputWriteList:
        """Convert these read versions of preprocessor input to the writing versions."""
        return PreprocessorInputWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PreprocessorInputWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PreprocessorInputWriteList(DomainModelWriteList[PreprocessorInputWrite]):
    """List of preprocessor inputs in the writing version."""

    _INSTANCE = PreprocessorInputWrite


class PreprocessorInputApplyList(PreprocessorInputWriteList): ...


def _create_preprocessor_input_filter(
    view_id: dm.ViewId,
    process_id: str | list[str] | None = None,
    process_id_prefix: str | None = None,
    min_process_step: int | None = None,
    max_process_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
    function_call_id: str | list[str] | None = None,
    function_call_id_prefix: str | None = None,
    scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    min_shop_start: datetime.datetime | None = None,
    max_shop_start: datetime.datetime | None = None,
    min_shop_end: datetime.datetime | None = None,
    max_shop_end: datetime.datetime | None = None,
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
    if scenario and isinstance(scenario, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("scenario"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": scenario}
            )
        )
    if scenario and isinstance(scenario, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("scenario"), value={"space": scenario[0], "externalId": scenario[1]}
            )
        )
    if scenario and isinstance(scenario, list) and isinstance(scenario[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("scenario"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in scenario],
            )
        )
    if scenario and isinstance(scenario, list) and isinstance(scenario[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("scenario"),
                values=[{"space": item[0], "externalId": item[1]} for item in scenario],
            )
        )
    if min_shop_start is not None or max_shop_start is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("shopStart"),
                gte=min_shop_start.isoformat(timespec="milliseconds") if min_shop_start else None,
                lte=max_shop_start.isoformat(timespec="milliseconds") if max_shop_start else None,
            )
        )
    if min_shop_end is not None or max_shop_end is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("shopEnd"),
                gte=min_shop_end.isoformat(timespec="milliseconds") if min_shop_end else None,
                lte=max_shop_end.isoformat(timespec="milliseconds") if max_shop_end else None,
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
