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
    from ._scenario import Scenario, ScenarioWrite


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


class SHOPTriggerInput(DomainModel):
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
        scenario: The scenario that is used in the shop run
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "SHOPTriggerInput"
    )
    process_id: str = Field(alias="processId")
    process_step: int = Field(alias="processStep")
    function_name: str = Field(alias="functionName")
    function_call_id: str = Field(alias="functionCallId")
    cog_shop_tag: Optional[str] = Field(None, alias="cogShopTag")
    scenario: Union[Scenario, str, dm.NodeId, None] = Field(None, repr=False)

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
            scenario=self.scenario.as_write() if isinstance(self.scenario, DomainModel) else self.scenario,
        )

    def as_apply(self) -> SHOPTriggerInputWrite:
        """Convert this read version of shop trigger input to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class SHOPTriggerInputWrite(DomainModelWrite):
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
        scenario: The scenario that is used in the shop run
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "SHOPTriggerInput"
    )
    process_id: str = Field(alias="processId")
    process_step: int = Field(alias="processStep")
    function_name: str = Field(alias="functionName")
    function_call_id: str = Field(alias="functionCallId")
    cog_shop_tag: Optional[str] = Field(None, alias="cogShopTag")
    scenario: Union[ScenarioWrite, str, dm.NodeId, None] = Field(None, repr=False)

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
            SHOPTriggerInput, dm.ViewId("sp_powerops_models", "SHOPTriggerInput", "1")
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

        if self.scenario is not None:
            properties["scenario"] = {
                "space": self.space if isinstance(self.scenario, str) else self.scenario.space,
                "externalId": self.scenario if isinstance(self.scenario, str) else self.scenario.external_id,
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

        if isinstance(self.scenario, DomainModelWrite):
            other_resources = self.scenario._to_instances_write(cache, view_by_read_class)
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
    scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
