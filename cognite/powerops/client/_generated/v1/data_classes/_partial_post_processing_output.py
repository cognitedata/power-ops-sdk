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

if TYPE_CHECKING:
    from ._alert import Alert, AlertGraphQL, AlertWrite
    from ._bid_matrix import BidMatrix, BidMatrixGraphQL, BidMatrixWrite
    from ._partial_post_processing_input import (
        PartialPostProcessingInput,
        PartialPostProcessingInputGraphQL,
        PartialPostProcessingInputWrite,
    )


__all__ = [
    "PartialPostProcessingOutput",
    "PartialPostProcessingOutputWrite",
    "PartialPostProcessingOutputApply",
    "PartialPostProcessingOutputList",
    "PartialPostProcessingOutputWriteList",
    "PartialPostProcessingOutputApplyList",
    "PartialPostProcessingOutputFields",
    "PartialPostProcessingOutputTextFields",
]


PartialPostProcessingOutputTextFields = Literal["process_id", "function_name", "function_call_id"]
PartialPostProcessingOutputFields = Literal["process_id", "process_step", "function_name", "function_call_id"]

_PARTIALPOSTPROCESSINGOUTPUT_PROPERTIES_BY_FIELD = {
    "process_id": "processId",
    "process_step": "processStep",
    "function_name": "functionName",
    "function_call_id": "functionCallId",
}


class PartialPostProcessingOutputGraphQL(GraphQLCore):
    """This represents the reading version of partial post processing output, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the partial post processing output.
        data_record: The data record of the partial post processing output node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        alerts: An array of calculation level Alerts.
        input_: The input field.
        partial_matrices: The processed partial bid matrices that are used to calculate the total bid matrix.
    """

    view_id = dm.ViewId("sp_powerops_models", "PartialPostProcessingOutput", "1")
    process_id: Optional[str] = Field(None, alias="processId")
    process_step: Optional[int] = Field(None, alias="processStep")
    function_name: Optional[str] = Field(None, alias="functionName")
    function_call_id: Optional[str] = Field(None, alias="functionCallId")
    alerts: Optional[list[AlertGraphQL]] = Field(default=None, repr=False)
    input_: Optional[PartialPostProcessingInputGraphQL] = Field(None, repr=False, alias="input")
    partial_matrices: Optional[list[BidMatrixGraphQL]] = Field(default=None, repr=False, alias="partialMatrices")

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

    @field_validator("alerts", "input_", "partial_matrices", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> PartialPostProcessingOutput:
        """Convert this GraphQL format of partial post processing output to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return PartialPostProcessingOutput(
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
            alerts=[alert.as_read() if isinstance(alert, GraphQLCore) else alert for alert in self.alerts or []],
            input_=self.input_.as_read() if isinstance(self.input_, GraphQLCore) else self.input_,
            partial_matrices=[
                partial_matrice.as_read() if isinstance(partial_matrice, GraphQLCore) else partial_matrice
                for partial_matrice in self.partial_matrices or []
            ],
        )

    def as_write(self) -> PartialPostProcessingOutputWrite:
        """Convert this GraphQL format of partial post processing output to the writing format."""
        return PartialPostProcessingOutputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            process_id=self.process_id,
            process_step=self.process_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
            input_=self.input_.as_write() if isinstance(self.input_, DomainModel) else self.input_,
            partial_matrices=[
                partial_matrice.as_write() if isinstance(partial_matrice, DomainModel) else partial_matrice
                for partial_matrice in self.partial_matrices or []
            ],
        )


class PartialPostProcessingOutput(DomainModel):
    """This represents the reading version of partial post processing output.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the partial post processing output.
        data_record: The data record of the partial post processing output node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        alerts: An array of calculation level Alerts.
        input_: The input field.
        partial_matrices: The processed partial bid matrices that are used to calculate the total bid matrix.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "PartialPostProcessingOutput"
    )
    process_id: str = Field(alias="processId")
    process_step: int = Field(alias="processStep")
    function_name: str = Field(alias="functionName")
    function_call_id: str = Field(alias="functionCallId")
    alerts: Union[list[Alert], list[str], list[dm.NodeId], None] = Field(default=None, repr=False)
    input_: Union[PartialPostProcessingInput, str, dm.NodeId, None] = Field(None, repr=False, alias="input")
    partial_matrices: Union[list[BidMatrix], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="partialMatrices"
    )

    def as_write(self) -> PartialPostProcessingOutputWrite:
        """Convert this read version of partial post processing output to the writing version."""
        return PartialPostProcessingOutputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            process_id=self.process_id,
            process_step=self.process_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
            input_=self.input_.as_write() if isinstance(self.input_, DomainModel) else self.input_,
            partial_matrices=[
                partial_matrice.as_write() if isinstance(partial_matrice, DomainModel) else partial_matrice
                for partial_matrice in self.partial_matrices or []
            ],
        )

    def as_apply(self) -> PartialPostProcessingOutputWrite:
        """Convert this read version of partial post processing output to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PartialPostProcessingOutputWrite(DomainModelWrite):
    """This represents the writing version of partial post processing output.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the partial post processing output.
        data_record: The data record of the partial post processing output node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        alerts: An array of calculation level Alerts.
        input_: The input field.
        partial_matrices: The processed partial bid matrices that are used to calculate the total bid matrix.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "PartialPostProcessingOutput"
    )
    process_id: str = Field(alias="processId")
    process_step: int = Field(alias="processStep")
    function_name: str = Field(alias="functionName")
    function_call_id: str = Field(alias="functionCallId")
    alerts: Union[list[AlertWrite], list[str], list[dm.NodeId], None] = Field(default=None, repr=False)
    input_: Union[PartialPostProcessingInputWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="input")
    partial_matrices: Union[list[BidMatrixWrite], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="partialMatrices"
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
            PartialPostProcessingOutput, dm.ViewId("sp_powerops_models", "PartialPostProcessingOutput", "1")
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

        if self.input_ is not None:
            properties["input"] = {
                "space": self.space if isinstance(self.input_, str) else self.input_.space,
                "externalId": self.input_ if isinstance(self.input_, str) else self.input_.external_id,
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

        edge_type = dm.DirectRelationReference("sp_powerops_types", "calculationIssue")
        for alert in self.alerts or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=alert,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("sp_powerops_types", "BidMatrix")
        for partial_matrice in self.partial_matrices or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=partial_matrice,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.input_, DomainModelWrite):
            other_resources = self.input_._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class PartialPostProcessingOutputApply(PartialPostProcessingOutputWrite):
    def __new__(cls, *args, **kwargs) -> PartialPostProcessingOutputApply:
        warnings.warn(
            "PartialPostProcessingOutputApply is deprecated and will be removed in v1.0. Use PartialPostProcessingOutputWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PartialPostProcessingOutput.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class PartialPostProcessingOutputList(DomainModelList[PartialPostProcessingOutput]):
    """List of partial post processing outputs in the read version."""

    _INSTANCE = PartialPostProcessingOutput

    def as_write(self) -> PartialPostProcessingOutputWriteList:
        """Convert these read versions of partial post processing output to the writing versions."""
        return PartialPostProcessingOutputWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PartialPostProcessingOutputWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PartialPostProcessingOutputWriteList(DomainModelWriteList[PartialPostProcessingOutputWrite]):
    """List of partial post processing outputs in the writing version."""

    _INSTANCE = PartialPostProcessingOutputWrite


class PartialPostProcessingOutputApplyList(PartialPostProcessingOutputWriteList): ...


def _create_partial_post_processing_output_filter(
    view_id: dm.ViewId,
    process_id: str | list[str] | None = None,
    process_id_prefix: str | None = None,
    min_process_step: int | None = None,
    max_process_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
    function_call_id: str | list[str] | None = None,
    function_call_id_prefix: str | None = None,
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
    if isinstance(function_call_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("functionCallId"), value=function_call_id))
    if function_call_id and isinstance(function_call_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("functionCallId"), values=function_call_id))
    if function_call_id_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("functionCallId"), value=function_call_id_prefix))
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
