from __future__ import annotations

import datetime
from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)


__all__ = ["Alert", "AlertApply", "AlertList", "AlertApplyList", "AlertFields", "AlertTextFields"]


AlertTextFields = Literal["title", "description", "severity", "alert_type", "calculation_run"]
AlertFields = Literal[
    "time", "title", "description", "severity", "alert_type", "status_code", "event_ids", "calculation_run"
]

_ALERT_PROPERTIES_BY_FIELD = {
    "time": "time",
    "title": "title",
    "description": "description",
    "severity": "severity",
    "alert_type": "alertType",
    "status_code": "statusCode",
    "event_ids": "eventIds",
    "calculation_run": "calculationRun",
}


class Alert(DomainModel):
    """This represents the reading version of alert.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the alert.
        time: Timestamp that the alert occured (within the workflow)
        title: Summary description of the alert
        description: Detailed description of the alert
        severity: CRITICAL (calculation could not completed) WARNING  (calculation completed, with major issue) INFO     (calculation completed, with minor issues)
        alert_type: Classification of the alert (not in current alerting implementation)
        status_code: Unique status code for the alert. May be used by the frontend to avoid use of hardcoded description (i.e. like a translation)
        event_ids: An array of associated alert CDF Events (e.g. SHOP Run events)
        calculation_run: The identifier of the parent Bid Calculation (required so tha alerts can be created befor the BidDocument)
        created_time: The created time of the alert node.
        last_updated_time: The last updated time of the alert node.
        deleted_time: If present, the deleted time of the alert node.
        version: The version of the alert node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    time: Optional[datetime.datetime] = None
    title: Optional[str] = None
    description: Optional[str] = None
    severity: Optional[str] = None
    alert_type: Optional[str] = Field(None, alias="alertType")
    status_code: Optional[int] = Field(None, alias="statusCode")
    event_ids: Optional[list[int]] = Field(None, alias="eventIds")
    calculation_run: Optional[str] = Field(None, alias="calculationRun")

    def as_apply(self) -> AlertApply:
        """Convert this read version of alert to the writing version."""
        return AlertApply(
            space=self.space,
            external_id=self.external_id,
            time=self.time,
            title=self.title,
            description=self.description,
            severity=self.severity,
            alert_type=self.alert_type,
            status_code=self.status_code,
            event_ids=self.event_ids,
            calculation_run=self.calculation_run,
        )


class AlertApply(DomainModelApply):
    """This represents the writing version of alert.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the alert.
        time: Timestamp that the alert occured (within the workflow)
        title: Summary description of the alert
        description: Detailed description of the alert
        severity: CRITICAL (calculation could not completed) WARNING  (calculation completed, with major issue) INFO     (calculation completed, with minor issues)
        alert_type: Classification of the alert (not in current alerting implementation)
        status_code: Unique status code for the alert. May be used by the frontend to avoid use of hardcoded description (i.e. like a translation)
        event_ids: An array of associated alert CDF Events (e.g. SHOP Run events)
        calculation_run: The identifier of the parent Bid Calculation (required so tha alerts can be created befor the BidDocument)
        existing_version: Fail the ingestion request if the alert version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    time: datetime.datetime
    title: str
    description: Optional[str] = None
    severity: Optional[str] = None
    alert_type: Optional[str] = Field(None, alias="alertType")
    status_code: Optional[int] = Field(None, alias="statusCode")
    event_ids: Optional[list[int]] = Field(None, alias="eventIds")
    calculation_run: Optional[str] = Field(None, alias="calculationRun")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "power-ops-shared", "Alert", "1"
        )

        properties = {}
        if self.time is not None:
            properties["time"] = self.time.isoformat(timespec="milliseconds")
        if self.title is not None:
            properties["title"] = self.title
        if self.description is not None:
            properties["description"] = self.description
        if self.severity is not None:
            properties["severity"] = self.severity
        if self.alert_type is not None:
            properties["alertType"] = self.alert_type
        if self.status_code is not None:
            properties["statusCode"] = self.status_code
        if self.event_ids is not None:
            properties["eventIds"] = self.event_ids
        if self.calculation_run is not None:
            properties["calculationRun"] = self.calculation_run

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        return resources


class AlertList(DomainModelList[Alert]):
    """List of alerts in the read version."""

    _INSTANCE = Alert

    def as_apply(self) -> AlertApplyList:
        """Convert these read versions of alert to the writing versions."""
        return AlertApplyList([node.as_apply() for node in self.data])


class AlertApplyList(DomainModelApplyList[AlertApply]):
    """List of alerts in the writing version."""

    _INSTANCE = AlertApply


def _create_alert_filter(
    view_id: dm.ViewId,
    min_time: datetime.datetime | None = None,
    max_time: datetime.datetime | None = None,
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
    filters = []
    if min_time or max_time:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("time"),
                gte=min_time.isoformat(timespec="milliseconds") if min_time else None,
                lte=max_time.isoformat(timespec="milliseconds") if max_time else None,
            )
        )
    if title and isinstance(title, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("title"), value=title))
    if title and isinstance(title, list):
        filters.append(dm.filters.In(view_id.as_property_ref("title"), values=title))
    if title_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("title"), value=title_prefix))
    if description and isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if severity and isinstance(severity, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("severity"), value=severity))
    if severity and isinstance(severity, list):
        filters.append(dm.filters.In(view_id.as_property_ref("severity"), values=severity))
    if severity_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("severity"), value=severity_prefix))
    if alert_type and isinstance(alert_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("alertType"), value=alert_type))
    if alert_type and isinstance(alert_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("alertType"), values=alert_type))
    if alert_type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("alertType"), value=alert_type_prefix))
    if min_status_code or max_status_code:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("statusCode"), gte=min_status_code, lte=max_status_code)
        )
    if calculation_run and isinstance(calculation_run, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("calculationRun"), value=calculation_run))
    if calculation_run and isinstance(calculation_run, list):
        filters.append(dm.filters.In(view_id.as_property_ref("calculationRun"), values=calculation_run))
    if calculation_run_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("calculationRun"), value=calculation_run_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
