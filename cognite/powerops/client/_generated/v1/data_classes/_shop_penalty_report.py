from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

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
    IntFilter,
    TimestampFilter,
)
from cognite.powerops.client._generated.v1.data_classes._alert import Alert, AlertWrite


__all__ = [
    "ShopPenaltyReport",
    "ShopPenaltyReportWrite",
    "ShopPenaltyReportList",
    "ShopPenaltyReportWriteList",
    "ShopPenaltyReportFields",
    "ShopPenaltyReportTextFields",
    "ShopPenaltyReportGraphQL",
]


ShopPenaltyReportTextFields = Literal["external_id", "workflow_execution_id", "title", "description", "severity", "alert_type", "calculation_run"]
ShopPenaltyReportFields = Literal["external_id", "time", "workflow_execution_id", "title", "description", "severity", "alert_type", "status_code", "event_ids", "calculation_run", "penalties"]

_SHOPPENALTYREPORT_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "time": "time",
    "workflow_execution_id": "workflowExecutionId",
    "title": "title",
    "description": "description",
    "severity": "severity",
    "alert_type": "alertType",
    "status_code": "statusCode",
    "event_ids": "eventIds",
    "calculation_run": "calculationRun",
    "penalties": "penalties",
}


class ShopPenaltyReportGraphQL(GraphQLCore):
    """This represents the reading version of shop penalty report, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop penalty report.
        data_record: The data record of the shop penalty report node.
        time: Timestamp that the alert occurred (within the workflow)
        workflow_execution_id: Process ID in the workflow that the alert is related to
        title: Summary description of the alert
        description: Detailed description of the alert
        severity: CRITICAL (calculation could not completed) WARNING  (calculation completed, with major issue) INFO
            (calculation completed, with minor issues)
        alert_type: Classification of the alert (not in current alerting implementation)
        status_code: Unique status code for the alert. May be used by the frontend to avoid use of hardcoded
            description (i.e. like a translation)
        event_ids: An array of associated alert CDF Events (e.g. SHOP Run events)
        calculation_run: The identifier of the parent Bid Calculation (required so that alerts can be created before
            the BidDocument)
        penalties: TODO
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopPenaltyReport", "1")
    time: Optional[datetime.datetime] = None
    workflow_execution_id: Optional[str] = Field(None, alias="workflowExecutionId")
    title: Optional[str] = None
    description: Optional[str] = None
    severity: Optional[str] = None
    alert_type: Optional[str] = Field(None, alias="alertType")
    status_code: Optional[int] = Field(None, alias="statusCode")
    event_ids: Optional[list[int]] = Field(None, alias="eventIds")
    calculation_run: Optional[str] = Field(None, alias="calculationRun")
    penalties: Optional[list[dict]] = None

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



    def as_read(self) -> ShopPenaltyReport:
        """Convert this GraphQL format of shop penalty report to the reading format."""
        return ShopPenaltyReport.model_validate(as_read_args(self))

    def as_write(self) -> ShopPenaltyReportWrite:
        """Convert this GraphQL format of shop penalty report to the writing format."""
        return ShopPenaltyReportWrite.model_validate(as_write_args(self))


class ShopPenaltyReport(Alert):
    """This represents the reading version of shop penalty report.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop penalty report.
        data_record: The data record of the shop penalty report node.
        time: Timestamp that the alert occurred (within the workflow)
        workflow_execution_id: Process ID in the workflow that the alert is related to
        title: Summary description of the alert
        description: Detailed description of the alert
        severity: CRITICAL (calculation could not completed) WARNING  (calculation completed, with major issue) INFO
            (calculation completed, with minor issues)
        alert_type: Classification of the alert (not in current alerting implementation)
        status_code: Unique status code for the alert. May be used by the frontend to avoid use of hardcoded
            description (i.e. like a translation)
        event_ids: An array of associated alert CDF Events (e.g. SHOP Run events)
        calculation_run: The identifier of the parent Bid Calculation (required so that alerts can be created before
            the BidDocument)
        penalties: TODO
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopPenaltyReport", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopPenaltyReport")
    penalties: Optional[list[dict]] = None


    def as_write(self) -> ShopPenaltyReportWrite:
        """Convert this read version of shop penalty report to the writing version."""
        return ShopPenaltyReportWrite.model_validate(as_write_args(self))



class ShopPenaltyReportWrite(AlertWrite):
    """This represents the writing version of shop penalty report.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop penalty report.
        data_record: The data record of the shop penalty report node.
        time: Timestamp that the alert occurred (within the workflow)
        workflow_execution_id: Process ID in the workflow that the alert is related to
        title: Summary description of the alert
        description: Detailed description of the alert
        severity: CRITICAL (calculation could not completed) WARNING  (calculation completed, with major issue) INFO
            (calculation completed, with minor issues)
        alert_type: Classification of the alert (not in current alerting implementation)
        status_code: Unique status code for the alert. May be used by the frontend to avoid use of hardcoded
            description (i.e. like a translation)
        event_ids: An array of associated alert CDF Events (e.g. SHOP Run events)
        calculation_run: The identifier of the parent Bid Calculation (required so that alerts can be created before
            the BidDocument)
        penalties: TODO
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("alert_type", "calculation_run", "description", "event_ids", "penalties", "severity", "status_code", "time", "title", "workflow_execution_id",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopPenaltyReport", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "ShopPenaltyReport")
    penalties: Optional[list[dict]] = None



class ShopPenaltyReportList(DomainModelList[ShopPenaltyReport]):
    """List of shop penalty reports in the read version."""

    _INSTANCE = ShopPenaltyReport
    def as_write(self) -> ShopPenaltyReportWriteList:
        """Convert these read versions of shop penalty report to the writing versions."""
        return ShopPenaltyReportWriteList([node.as_write() for node in self.data])



class ShopPenaltyReportWriteList(DomainModelWriteList[ShopPenaltyReportWrite]):
    """List of shop penalty reports in the writing version."""

    _INSTANCE = ShopPenaltyReportWrite


def _create_shop_penalty_report_filter(
    view_id: dm.ViewId,
    min_time: datetime.datetime | None = None,
    max_time: datetime.datetime | None = None,
    workflow_execution_id: str | list[str] | None = None,
    workflow_execution_id_prefix: str | None = None,
    title: str | list[str] | None = None,
    title_prefix: str | None = None,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    severity: str | list[str] | None = None,
    severity_prefix: str | None = None,
    alert_type: str | list[str] | None = None,
    alert_type_prefix: str | None = None,
    min_status_code: int | None = None,
    max_status_code: int | None = None,
    calculation_run: str | list[str] | None = None,
    calculation_run_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if min_time is not None or max_time is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("time"), gte=min_time.isoformat(timespec="milliseconds") if min_time else None, lte=max_time.isoformat(timespec="milliseconds") if max_time else None))
    if isinstance(workflow_execution_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("workflowExecutionId"), value=workflow_execution_id))
    if workflow_execution_id and isinstance(workflow_execution_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("workflowExecutionId"), values=workflow_execution_id))
    if workflow_execution_id_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("workflowExecutionId"), value=workflow_execution_id_prefix))
    if isinstance(title, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("title"), value=title))
    if title and isinstance(title, list):
        filters.append(dm.filters.In(view_id.as_property_ref("title"), values=title))
    if title_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("title"), value=title_prefix))
    if isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if isinstance(severity, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("severity"), value=severity))
    if severity and isinstance(severity, list):
        filters.append(dm.filters.In(view_id.as_property_ref("severity"), values=severity))
    if severity_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("severity"), value=severity_prefix))
    if isinstance(alert_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("alertType"), value=alert_type))
    if alert_type and isinstance(alert_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("alertType"), values=alert_type))
    if alert_type_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("alertType"), value=alert_type_prefix))
    if min_status_code is not None or max_status_code is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("statusCode"), gte=min_status_code, lte=max_status_code))
    if isinstance(calculation_run, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("calculationRun"), value=calculation_run))
    if calculation_run and isinstance(calculation_run, list):
        filters.append(dm.filters.In(view_id.as_property_ref("calculationRun"), values=calculation_run))
    if calculation_run_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("calculationRun"), value=calculation_run_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _ShopPenaltyReportQuery(NodeQueryCore[T_DomainModelList, ShopPenaltyReportList]):
    _view_id = ShopPenaltyReport._view_id
    _result_cls = ShopPenaltyReport
    _result_list_cls_end = ShopPenaltyReportList

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

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.time = TimestampFilter(self, self._view_id.as_property_ref("time"))
        self.workflow_execution_id = StringFilter(self, self._view_id.as_property_ref("workflowExecutionId"))
        self.title = StringFilter(self, self._view_id.as_property_ref("title"))
        self.description = StringFilter(self, self._view_id.as_property_ref("description"))
        self.severity = StringFilter(self, self._view_id.as_property_ref("severity"))
        self.alert_type = StringFilter(self, self._view_id.as_property_ref("alertType"))
        self.status_code = IntFilter(self, self._view_id.as_property_ref("statusCode"))
        self.calculation_run = StringFilter(self, self._view_id.as_property_ref("calculationRun"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.time,
            self.workflow_execution_id,
            self.title,
            self.description,
            self.severity,
            self.alert_type,
            self.status_code,
            self.calculation_run,
        ])

    def list_shop_penalty_report(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopPenaltyReportList:
        return self._list(limit=limit)


class ShopPenaltyReportQuery(_ShopPenaltyReportQuery[ShopPenaltyReportList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopPenaltyReportList)
