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
    from ._alert import Alert, AlertWrite
    from ._preprocessor_input import PreprocessorInput
    from ._scenario import Scenario, ScenarioWrite


__all__ = ["PreprocessorOutput", "PreprocessorOutputList", "PreprocessorOutputFields", "PreprocessorOutputTextFields"]


PreprocessorOutputTextFields = Literal["process_id", "function_name"]
PreprocessorOutputFields = Literal["process_id", "process_step", "function_name"]

_PREPROCESSOROUTPUT_PROPERTIES_BY_FIELD = {
    "process_id": "processId",
    "process_step": "processStep",
    "function_name": "functionName",
}


class PreprocessorOutput(DomainModel):
    """This represents the reading version of preprocessor output.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the preprocessor output.
        data_record: The data record of the preprocessor output node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        alerts: An array of calculation level Alerts.
        scenario: The prepped and processed scenario to send to shop trigger
        input_: The prepped and processed scenario to send to shop trigger
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "PreprocessorOutput"
    )
    process_id: str = Field(alias="processId")
    process_step: int = Field(alias="processStep")
    function_name: str = Field(alias="functionName")
    alerts: Union[list[Alert], list[str], None] = Field(default=None, repr=False)
    scenario: Union[Scenario, str, dm.NodeId, None] = Field(None, repr=False)
    input_: Union[PreprocessorInput, str, dm.NodeId, None] = Field(None, repr=False, alias="input")


class PreprocessorOutputList(DomainModelList[PreprocessorOutput]):
    """List of preprocessor outputs in the read version."""

    _INSTANCE = PreprocessorOutput


def _create_preprocessor_output_filter(
    view_id: dm.ViewId,
    process_id: str | list[str] | None = None,
    process_id_prefix: str | None = None,
    min_process_step: int | None = None,
    max_process_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
    scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    input_: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if input_ and isinstance(input_, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("input"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": input_}
            )
        )
    if input_ and isinstance(input_, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("input"), value={"space": input_[0], "externalId": input_[1]})
        )
    if input_ and isinstance(input_, list) and isinstance(input_[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("input"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in input_],
            )
        )
    if input_ and isinstance(input_, list) and isinstance(input_[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("input"), values=[{"space": item[0], "externalId": item[1]} for item in input_]
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
