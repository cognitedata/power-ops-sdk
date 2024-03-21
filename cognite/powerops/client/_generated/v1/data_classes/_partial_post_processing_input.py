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
    from ._bid_matrix_raw import BidMatrixRaw, BidMatrixRawGraphQL, BidMatrixRawWrite
    from ._market_configuration import MarketConfiguration, MarketConfigurationGraphQL, MarketConfigurationWrite


__all__ = [
    "PartialPostProcessingInput",
    "PartialPostProcessingInputWrite",
    "PartialPostProcessingInputApply",
    "PartialPostProcessingInputList",
    "PartialPostProcessingInputWriteList",
    "PartialPostProcessingInputApplyList",
    "PartialPostProcessingInputFields",
    "PartialPostProcessingInputTextFields",
]


PartialPostProcessingInputTextFields = Literal["process_id", "function_name", "function_call_id"]
PartialPostProcessingInputFields = Literal["process_id", "process_step", "function_name", "function_call_id"]

_PARTIALPOSTPROCESSINGINPUT_PROPERTIES_BY_FIELD = {
    "process_id": "processId",
    "process_step": "processStep",
    "function_name": "functionName",
    "function_call_id": "functionCallId",
}


class PartialPostProcessingInputGraphQL(GraphQLCore):
    """This represents the reading version of partial post processing input, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the partial post processing input.
        data_record: The data record of the partial post processing input node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        market_config: The market config field.
        partial_bid_matrices_raw: The partial bid matrices that needs post processing.
    """

    view_id = dm.ViewId("sp_powerops_models", "PartialPostProcessingInput", "1")
    process_id: Optional[str] = Field(None, alias="processId")
    process_step: Optional[int] = Field(None, alias="processStep")
    function_name: Optional[str] = Field(None, alias="functionName")
    function_call_id: Optional[str] = Field(None, alias="functionCallId")
    market_config: Optional[MarketConfigurationGraphQL] = Field(None, repr=False, alias="marketConfig")
    partial_bid_matrices_raw: Optional[list[BidMatrixRawGraphQL]] = Field(
        default=None, repr=False, alias="partialBidMatricesRaw"
    )

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

    @field_validator("market_config", "partial_bid_matrices_raw", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> PartialPostProcessingInput:
        """Convert this GraphQL format of partial post processing input to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return PartialPostProcessingInput(
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
            market_config=(
                self.market_config.as_read() if isinstance(self.market_config, GraphQLCore) else self.market_config
            ),
            partial_bid_matrices_raw=[
                (
                    partial_bid_matrices_raw.as_read()
                    if isinstance(partial_bid_matrices_raw, GraphQLCore)
                    else partial_bid_matrices_raw
                )
                for partial_bid_matrices_raw in self.partial_bid_matrices_raw or []
            ],
        )

    def as_write(self) -> PartialPostProcessingInputWrite:
        """Convert this GraphQL format of partial post processing input to the writing format."""
        return PartialPostProcessingInputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            process_id=self.process_id,
            process_step=self.process_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            market_config=(
                self.market_config.as_write() if isinstance(self.market_config, DomainModel) else self.market_config
            ),
            partial_bid_matrices_raw=[
                (
                    partial_bid_matrices_raw.as_write()
                    if isinstance(partial_bid_matrices_raw, DomainModel)
                    else partial_bid_matrices_raw
                )
                for partial_bid_matrices_raw in self.partial_bid_matrices_raw or []
            ],
        )


class PartialPostProcessingInput(DomainModel):
    """This represents the reading version of partial post processing input.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the partial post processing input.
        data_record: The data record of the partial post processing input node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        market_config: The market config field.
        partial_bid_matrices_raw: The partial bid matrices that needs post processing.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "PartialPostProcessingInput"
    )
    process_id: str = Field(alias="processId")
    process_step: int = Field(alias="processStep")
    function_name: str = Field(alias="functionName")
    function_call_id: str = Field(alias="functionCallId")
    market_config: Union[MarketConfiguration, str, dm.NodeId, None] = Field(None, repr=False, alias="marketConfig")
    partial_bid_matrices_raw: Union[list[BidMatrixRaw], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="partialBidMatricesRaw"
    )

    def as_write(self) -> PartialPostProcessingInputWrite:
        """Convert this read version of partial post processing input to the writing version."""
        return PartialPostProcessingInputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            process_id=self.process_id,
            process_step=self.process_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            market_config=(
                self.market_config.as_write() if isinstance(self.market_config, DomainModel) else self.market_config
            ),
            partial_bid_matrices_raw=[
                (
                    partial_bid_matrices_raw.as_write()
                    if isinstance(partial_bid_matrices_raw, DomainModel)
                    else partial_bid_matrices_raw
                )
                for partial_bid_matrices_raw in self.partial_bid_matrices_raw or []
            ],
        )

    def as_apply(self) -> PartialPostProcessingInputWrite:
        """Convert this read version of partial post processing input to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PartialPostProcessingInputWrite(DomainModelWrite):
    """This represents the writing version of partial post processing input.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the partial post processing input.
        data_record: The data record of the partial post processing input node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        market_config: The market config field.
        partial_bid_matrices_raw: The partial bid matrices that needs post processing.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "PartialPostProcessingInput"
    )
    process_id: str = Field(alias="processId")
    process_step: int = Field(alias="processStep")
    function_name: str = Field(alias="functionName")
    function_call_id: str = Field(alias="functionCallId")
    market_config: Union[MarketConfigurationWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="marketConfig")
    partial_bid_matrices_raw: Union[list[BidMatrixRawWrite], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="partialBidMatricesRaw"
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
            PartialPostProcessingInput, dm.ViewId("sp_powerops_models", "PartialPostProcessingInput", "1")
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

        if self.market_config is not None:
            properties["marketConfig"] = {
                "space": self.space if isinstance(self.market_config, str) else self.market_config.space,
                "externalId": (
                    self.market_config if isinstance(self.market_config, str) else self.market_config.external_id
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

        edge_type = dm.DirectRelationReference("sp_powerops_types", "partialBidMatricesRaw")
        for partial_bid_matrices_raw in self.partial_bid_matrices_raw or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=partial_bid_matrices_raw,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.market_config, DomainModelWrite):
            other_resources = self.market_config._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class PartialPostProcessingInputApply(PartialPostProcessingInputWrite):
    def __new__(cls, *args, **kwargs) -> PartialPostProcessingInputApply:
        warnings.warn(
            "PartialPostProcessingInputApply is deprecated and will be removed in v1.0. Use PartialPostProcessingInputWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PartialPostProcessingInput.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class PartialPostProcessingInputList(DomainModelList[PartialPostProcessingInput]):
    """List of partial post processing inputs in the read version."""

    _INSTANCE = PartialPostProcessingInput

    def as_write(self) -> PartialPostProcessingInputWriteList:
        """Convert these read versions of partial post processing input to the writing versions."""
        return PartialPostProcessingInputWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PartialPostProcessingInputWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PartialPostProcessingInputWriteList(DomainModelWriteList[PartialPostProcessingInputWrite]):
    """List of partial post processing inputs in the writing version."""

    _INSTANCE = PartialPostProcessingInputWrite


class PartialPostProcessingInputApplyList(PartialPostProcessingInputWriteList): ...


def _create_partial_post_processing_input_filter(
    view_id: dm.ViewId,
    process_id: str | list[str] | None = None,
    process_id_prefix: str | None = None,
    min_process_step: int | None = None,
    max_process_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
    function_call_id: str | list[str] | None = None,
    function_call_id_prefix: str | None = None,
    market_config: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if market_config and isinstance(market_config, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("marketConfig"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": market_config},
            )
        )
    if market_config and isinstance(market_config, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("marketConfig"),
                value={"space": market_config[0], "externalId": market_config[1]},
            )
        )
    if market_config and isinstance(market_config, list) and isinstance(market_config[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("marketConfig"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in market_config],
            )
        )
    if market_config and isinstance(market_config, list) and isinstance(market_config[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("marketConfig"),
                values=[{"space": item[0], "externalId": item[1]} for item in market_config],
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
