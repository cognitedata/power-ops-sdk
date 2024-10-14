from __future__ import annotations

import datetime
import warnings
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

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


__all__ = [
    "Alert",
    "AlertWrite",
    "AlertApply",
    "AlertList",
    "AlertWriteList",
    "AlertApplyList",
    "AlertFields",
    "AlertTextFields",
    "AlertGraphQL",
]


AlertTextFields = Literal["workflow_execution_id", "title", "description", "severity", "alert_type", "calculation_run"]
AlertFields = Literal["time", "workflow_execution_id", "title", "description", "severity", "alert_type", "status_code", "event_ids", "calculation_run"]

_ALERT_PROPERTIES_BY_FIELD = {
    "time": "time",
    "workflow_execution_id": "workflowExecutionId",
    "title": "title",
    "description": "description",
    "severity": "severity",
    "alert_type": "alertType",
    "status_code": "statusCode",
    "event_ids": "eventIds",
    "calculation_run": "calculationRun",
}

class AlertGraphQL(GraphQLCore):
    """This represents the reading version of alert, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the alert.
        data_record: The data record of the alert node.
        time: Timestamp that the alert occurred (within the workflow)
        workflow_execution_id: Process ID in the workflow that the alert is related to
        title: Summary description of the alert
        description: Detailed description of the alert
        severity: CRITICAL (calculation could not completed) WARNING  (calculation completed, with major issue) INFO     (calculation completed, with minor issues)
        alert_type: Classification of the alert (not in current alerting implementation)
        status_code: Unique status code for the alert. May be used by the frontend to avoid use of hardcoded description (i.e. like a translation)
        event_ids: An array of associated alert CDF Events (e.g. SHOP Run events)
        calculation_run: The identifier of the parent Bid Calculation (required so that alerts can be created before the BidDocument)
    """
    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "Alert", "1")
    time: Optional[datetime.datetime] = None
    workflow_execution_id: Optional[str] = Field(None, alias="workflowExecutionId")
    title: Optional[str] = None
    description: Optional[str] = None
    severity: Optional[str] = None
    alert_type: Optional[str] = Field(None, alias="alertType")
    status_code: Optional[int] = Field(None, alias="statusCode")
    event_ids: Optional[list[int]] = Field(None, alias="eventIds")
    calculation_run: Optional[str] = Field(None, alias="calculationRun")

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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> Alert:
        """Convert this GraphQL format of alert to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Alert(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            time=self.time,
            workflow_execution_id=self.workflow_execution_id,
            title=self.title,
            description=self.description,
            severity=self.severity,
            alert_type=self.alert_type,
            status_code=self.status_code,
            event_ids=self.event_ids,
            calculation_run=self.calculation_run,
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> AlertWrite:
        """Convert this GraphQL format of alert to the writing format."""
        return AlertWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            time=self.time,
            workflow_execution_id=self.workflow_execution_id,
            title=self.title,
            description=self.description,
            severity=self.severity,
            alert_type=self.alert_type,
            status_code=self.status_code,
            event_ids=self.event_ids,
            calculation_run=self.calculation_run,
        )


class Alert(DomainModel):
    """This represents the reading version of alert.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the alert.
        data_record: The data record of the alert node.
        time: Timestamp that the alert occurred (within the workflow)
        workflow_execution_id: Process ID in the workflow that the alert is related to
        title: Summary description of the alert
        description: Detailed description of the alert
        severity: CRITICAL (calculation could not completed) WARNING  (calculation completed, with major issue) INFO     (calculation completed, with minor issues)
        alert_type: Classification of the alert (not in current alerting implementation)
        status_code: Unique status code for the alert. May be used by the frontend to avoid use of hardcoded description (i.e. like a translation)
        event_ids: An array of associated alert CDF Events (e.g. SHOP Run events)
        calculation_run: The identifier of the parent Bid Calculation (required so that alerts can be created before the BidDocument)
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "Alert", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    time: datetime.datetime
    workflow_execution_id: Optional[str] = Field(None, alias="workflowExecutionId")
    title: str
    description: Optional[str] = None
    severity: Optional[str] = None
    alert_type: Optional[str] = Field(None, alias="alertType")
    status_code: Optional[int] = Field(None, alias="statusCode")
    event_ids: Optional[list[int]] = Field(None, alias="eventIds")
    calculation_run: Optional[str] = Field(None, alias="calculationRun")

    def as_write(self) -> AlertWrite:
        """Convert this read version of alert to the writing version."""
        return AlertWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            time=self.time,
            workflow_execution_id=self.workflow_execution_id,
            title=self.title,
            description=self.description,
            severity=self.severity,
            alert_type=self.alert_type,
            status_code=self.status_code,
            event_ids=self.event_ids,
            calculation_run=self.calculation_run,
        )

    def as_apply(self) -> AlertWrite:
        """Convert this read version of alert to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class AlertWrite(DomainModelWrite):
    """This represents the writing version of alert.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the alert.
        data_record: The data record of the alert node.
        time: Timestamp that the alert occurred (within the workflow)
        workflow_execution_id: Process ID in the workflow that the alert is related to
        title: Summary description of the alert
        description: Detailed description of the alert
        severity: CRITICAL (calculation could not completed) WARNING  (calculation completed, with major issue) INFO     (calculation completed, with minor issues)
        alert_type: Classification of the alert (not in current alerting implementation)
        status_code: Unique status code for the alert. May be used by the frontend to avoid use of hardcoded description (i.e. like a translation)
        event_ids: An array of associated alert CDF Events (e.g. SHOP Run events)
        calculation_run: The identifier of the parent Bid Calculation (required so that alerts can be created before the BidDocument)
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "Alert", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    time: datetime.datetime
    workflow_execution_id: Optional[str] = Field(None, alias="workflowExecutionId")
    title: str
    description: Optional[str] = None
    severity: Optional[str] = None
    alert_type: Optional[str] = Field(None, alias="alertType")
    status_code: Optional[int] = Field(None, alias="statusCode")
    event_ids: Optional[list[int]] = Field(None, alias="eventIds")
    calculation_run: Optional[str] = Field(None, alias="calculationRun")

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

        if self.time is not None:
            properties["time"] = self.time.isoformat(timespec="milliseconds") if self.time else None

        if self.workflow_execution_id is not None or write_none:
            properties["workflowExecutionId"] = self.workflow_execution_id

        if self.title is not None:
            properties["title"] = self.title

        if self.description is not None or write_none:
            properties["description"] = self.description

        if self.severity is not None or write_none:
            properties["severity"] = self.severity

        if self.alert_type is not None or write_none:
            properties["alertType"] = self.alert_type

        if self.status_code is not None or write_none:
            properties["statusCode"] = self.status_code

        if self.event_ids is not None or write_none:
            properties["eventIds"] = self.event_ids

        if self.calculation_run is not None or write_none:
            properties["calculationRun"] = self.calculation_run


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



        return resources


class AlertApply(AlertWrite):
    def __new__(cls, *args, **kwargs) -> AlertApply:
        warnings.warn(
            "AlertApply is deprecated and will be removed in v1.0. Use AlertWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Alert.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class AlertList(DomainModelList[Alert]):
    """List of alerts in the read version."""

    _INSTANCE = Alert

    def as_write(self) -> AlertWriteList:
        """Convert these read versions of alert to the writing versions."""
        return AlertWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> AlertWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class AlertWriteList(DomainModelWriteList[AlertWrite]):
    """List of alerts in the writing version."""

    _INSTANCE = AlertWrite

class AlertApplyList(AlertWriteList): ...



def _create_alert_filter(
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
