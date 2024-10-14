from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Literal,  no_type_check, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
)

if TYPE_CHECKING:
    from ._alert import Alert, AlertGraphQL, AlertWrite
    from ._shop_case import ShopCase, ShopCaseGraphQL, ShopCaseWrite
    from ._shop_time_series import ShopTimeSeries, ShopTimeSeriesGraphQL, ShopTimeSeriesWrite


__all__ = [
    "ShopResult",
    "ShopResultWrite",
    "ShopResultApply",
    "ShopResultList",
    "ShopResultWriteList",
    "ShopResultApplyList",
    "ShopResultFields",
    "ShopResultTextFields",
    "ShopResultGraphQL",
]


ShopResultTextFields = Literal["pre_run", "post_run", "messages", "cplex_logs"]
ShopResultFields = Literal["objective_value", "pre_run", "post_run", "messages", "cplex_logs"]

_SHOPRESULT_PROPERTIES_BY_FIELD = {
    "objective_value": "objectiveValue",
    "pre_run": "preRun",
    "post_run": "postRun",
    "messages": "messages",
    "cplex_logs": "cplexLogs",
}

class ShopResultGraphQL(GraphQLCore):
    """This represents the reading version of shop result, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop result.
        data_record: The data record of the shop result node.
        case: The case that was used to produce this result
        objective_value: The sequence of the objective function
        pre_run: The pre-run data for the SHOP run
        post_run: The post-run data for the SHOP run
        messages: The messages from the SHOP run
        cplex_logs: The logs from CPLEX
        alerts: An array of calculation level Alerts.
        output_time_series: TODO
    """
    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopResult", "1")
    case: Optional[ShopCaseGraphQL] = Field(default=None, repr=False)
    objective_value: Optional[dict] = Field(None, alias="objectiveValue")
    pre_run: Union[dict, None] = Field(None, alias="preRun")
    post_run: Union[dict, None] = Field(None, alias="postRun")
    messages: Union[dict, None] = None
    cplex_logs: Union[dict, None] = Field(None, alias="cplexLogs")
    alerts: Optional[list[AlertGraphQL]] = Field(default=None, repr=False)
    output_time_series: Optional[list[ShopTimeSeriesGraphQL]] = Field(default=None, repr=False, alias="outputTimeSeries")

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
    @field_validator("case", "alerts", "output_time_series", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ShopResult:
        """Convert this GraphQL format of shop result to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ShopResult(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            case=self.case.as_read() if isinstance(self.case, GraphQLCore) else self.case,
            objective_value=self.objective_value,
            pre_run=self.pre_run["externalId"] if self.pre_run and "externalId" in self.pre_run else None,
            post_run=self.post_run["externalId"] if self.post_run and "externalId" in self.post_run else None,
            messages=self.messages["externalId"] if self.messages and "externalId" in self.messages else None,
            cplex_logs=self.cplex_logs["externalId"] if self.cplex_logs and "externalId" in self.cplex_logs else None,
            alerts=[alert.as_read() for alert in self.alerts or []],
            output_time_series=[output_time_series.as_read() for output_time_series in self.output_time_series or []],
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopResultWrite:
        """Convert this GraphQL format of shop result to the writing format."""
        return ShopResultWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            case=self.case.as_write() if isinstance(self.case, GraphQLCore) else self.case,
            objective_value=self.objective_value,
            pre_run=self.pre_run["externalId"] if self.pre_run and "externalId" in self.pre_run else None,
            post_run=self.post_run["externalId"] if self.post_run and "externalId" in self.post_run else None,
            messages=self.messages["externalId"] if self.messages and "externalId" in self.messages else None,
            cplex_logs=self.cplex_logs["externalId"] if self.cplex_logs and "externalId" in self.cplex_logs else None,
            alerts=[alert.as_write() for alert in self.alerts or []],
            output_time_series=[output_time_series.as_write() for output_time_series in self.output_time_series or []],
        )


class ShopResult(DomainModel):
    """This represents the reading version of shop result.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop result.
        data_record: The data record of the shop result node.
        case: The case that was used to produce this result
        objective_value: The sequence of the objective function
        pre_run: The pre-run data for the SHOP run
        post_run: The post-run data for the SHOP run
        messages: The messages from the SHOP run
        cplex_logs: The logs from CPLEX
        alerts: An array of calculation level Alerts.
        output_time_series: TODO
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopResult", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    case: Union[ShopCase, str, dm.NodeId, None] = Field(default=None, repr=False)
    objective_value: Optional[dict] = Field(None, alias="objectiveValue")
    pre_run: Union[str, None] = Field(None, alias="preRun")
    post_run: Union[str, None] = Field(None, alias="postRun")
    messages: Union[str, None] = None
    cplex_logs: Union[str, None] = Field(None, alias="cplexLogs")
    alerts: Optional[list[Union[Alert, str, dm.NodeId]]] = Field(default=None, repr=False)
    output_time_series: Optional[list[Union[ShopTimeSeries, str, dm.NodeId]]] = Field(default=None, repr=False, alias="outputTimeSeries")

    def as_write(self) -> ShopResultWrite:
        """Convert this read version of shop result to the writing version."""
        return ShopResultWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            case=self.case.as_write() if isinstance(self.case, DomainModel) else self.case,
            objective_value=self.objective_value,
            pre_run=self.pre_run,
            post_run=self.post_run,
            messages=self.messages,
            cplex_logs=self.cplex_logs,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
            output_time_series=[output_time_series.as_write() if isinstance(output_time_series, DomainModel) else output_time_series for output_time_series in self.output_time_series or []],
        )

    def as_apply(self) -> ShopResultWrite:
        """Convert this read version of shop result to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopResultWrite(DomainModelWrite):
    """This represents the writing version of shop result.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop result.
        data_record: The data record of the shop result node.
        case: The case that was used to produce this result
        objective_value: The sequence of the objective function
        pre_run: The pre-run data for the SHOP run
        post_run: The post-run data for the SHOP run
        messages: The messages from the SHOP run
        cplex_logs: The logs from CPLEX
        alerts: An array of calculation level Alerts.
        output_time_series: TODO
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopResult", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    case: Union[ShopCaseWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    objective_value: Optional[dict] = Field(None, alias="objectiveValue")
    pre_run: Union[str, None] = Field(None, alias="preRun")
    post_run: Union[str, None] = Field(None, alias="postRun")
    messages: Union[str, None] = None
    cplex_logs: Union[str, None] = Field(None, alias="cplexLogs")
    alerts: Optional[list[Union[AlertWrite, str, dm.NodeId]]] = Field(default=None, repr=False)
    output_time_series: Optional[list[Union[ShopTimeSeriesWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="outputTimeSeries")

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

        if self.case is not None:
            properties["case"] = {
                "space":  self.space if isinstance(self.case, str) else self.case.space,
                "externalId": self.case if isinstance(self.case, str) else self.case.external_id,
            }

        if self.objective_value is not None or write_none:
            properties["objectiveValue"] = self.objective_value

        if self.pre_run is not None or write_none:
            properties["preRun"] = self.pre_run

        if self.post_run is not None or write_none:
            properties["postRun"] = self.post_run

        if self.messages is not None or write_none:
            properties["messages"] = self.messages

        if self.cplex_logs is not None or write_none:
            properties["cplexLogs"] = self.cplex_logs


        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                )],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())



        edge_type = dm.DirectRelationReference("power_ops_types", "calculationIssue")
        for alert in self.alerts or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=alert,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("power_ops_types", "ShopResult.outputTimeSeries")
        for output_time_series in self.output_time_series or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=output_time_series,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.case, DomainModelWrite):
            other_resources = self.case._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class ShopResultApply(ShopResultWrite):
    def __new__(cls, *args, **kwargs) -> ShopResultApply:
        warnings.warn(
            "ShopResultApply is deprecated and will be removed in v1.0. Use ShopResultWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ShopResult.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ShopResultList(DomainModelList[ShopResult]):
    """List of shop results in the read version."""

    _INSTANCE = ShopResult

    def as_write(self) -> ShopResultWriteList:
        """Convert these read versions of shop result to the writing versions."""
        return ShopResultWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ShopResultWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopResultWriteList(DomainModelWriteList[ShopResultWrite]):
    """List of shop results in the writing version."""

    _INSTANCE = ShopResultWrite

class ShopResultApplyList(ShopResultWriteList): ...



def _create_shop_result_filter(
    view_id: dm.ViewId,
    case: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if case and isinstance(case, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("case"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": case}))
    if case and isinstance(case, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("case"), value={"space": case[0], "externalId": case[1]}))
    if case and isinstance(case, list) and isinstance(case[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("case"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in case]))
    if case and isinstance(case, list) and isinstance(case[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("case"), values=[{"space": item[0], "externalId": item[1]} for item in case]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
