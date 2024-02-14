from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    ResourcesWrite,
)

if TYPE_CHECKING:
    from ._scenario_raw import ScenarioRaw, ScenarioRawWrite


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
PreprocessorInputFields = Literal["process_id", "process_step", "function_name", "function_call_id"]

_PREPROCESSORINPUT_PROPERTIES_BY_FIELD = {
    "process_id": "processId",
    "process_step": "processStep",
    "function_name": "functionName",
    "function_call_id": "functionCallId",
}


class PreprocessorInput(DomainModel):
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
        scenario_raw: The scenario that needs preprocessing before being sent to shop (has isReady flag set to false)
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "PreprocessorInput"
    )
    process_id: str = Field(alias="processId")
    process_step: int = Field(alias="processStep")
    function_name: str = Field(alias="functionName")
    function_call_id: str = Field(alias="functionCallId")
    scenario_raw: Union[ScenarioRaw, str, dm.NodeId, None] = Field(None, repr=False, alias="scenarioRaw")

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
            scenario_raw=(
                self.scenario_raw.as_write() if isinstance(self.scenario_raw, DomainModel) else self.scenario_raw
            ),
        )

    def as_apply(self) -> PreprocessorInputWrite:
        """Convert this read version of preprocessor input to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PreprocessorInputWrite(DomainModelWrite):
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
        scenario_raw: The scenario that needs preprocessing before being sent to shop (has isReady flag set to false)
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "PreprocessorInput"
    )
    process_id: str = Field(alias="processId")
    process_step: int = Field(alias="processStep")
    function_name: str = Field(alias="functionName")
    function_call_id: str = Field(alias="functionCallId")
    scenario_raw: Union[ScenarioRawWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="scenarioRaw")

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            PreprocessorInput, dm.ViewId("sp_powerops_models", "PreprocessorInput", "1")
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

        if self.scenario_raw is not None:
            properties["scenarioRaw"] = {
                "space": self.space if isinstance(self.scenario_raw, str) else self.scenario_raw.space,
                "externalId": (
                    self.scenario_raw if isinstance(self.scenario_raw, str) else self.scenario_raw.external_id
                ),
            }

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.data_record.existing_version,
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

        if isinstance(self.scenario_raw, DomainModelWrite):
            other_resources = self.scenario_raw._to_instances_write(cache, view_by_read_class)
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
    scenario_raw: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if scenario_raw and isinstance(scenario_raw, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("scenarioRaw"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": scenario_raw},
            )
        )
    if scenario_raw and isinstance(scenario_raw, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("scenarioRaw"), value={"space": scenario_raw[0], "externalId": scenario_raw[1]}
            )
        )
    if scenario_raw and isinstance(scenario_raw, list) and isinstance(scenario_raw[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("scenarioRaw"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in scenario_raw],
            )
        )
    if scenario_raw and isinstance(scenario_raw, list) and isinstance(scenario_raw[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("scenarioRaw"),
                values=[{"space": item[0], "externalId": item[1]} for item in scenario_raw],
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
