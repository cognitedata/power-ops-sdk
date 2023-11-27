from __future__ import annotations

import datetime
from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

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
    """This represent a read version of alert.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the alert.
        time: The time field.
        title: The title field.
        description: The description field.
        severity: The severity field.
        alert_type: The alert type field.
        status_code: The status code field.
        event_ids: The event id field.
        calculation_run: The calculation run field.
        created_time: The created time of the alert node.
        last_updated_time: The last updated time of the alert node.
        deleted_time: If present, the deleted time of the alert node.
        version: The version of the alert node.
    """

    space: str = "dayAheadFrontendContractModel"
    time: Optional[datetime.datetime] = None
    title: Optional[str] = None
    description: Optional[str] = None
    severity: Optional[str] = None
    alert_type: Optional[str] = Field(None, alias="alertType")
    status_code: Optional[int] = Field(None, alias="statusCode")
    event_ids: Optional[list[int]] = Field(None, alias="eventIds")
    calculation_run: Optional[str] = Field(None, alias="calculationRun")

    def as_apply(self) -> AlertApply:
        """Convert this read version of alert to a write version."""
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
    """This represent a write version of alert.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the alert.
        time: The time field.
        title: The title field.
        description: The description field.
        severity: The severity field.
        alert_type: The alert type field.
        status_code: The status code field.
        event_ids: The event id field.
        calculation_run: The calculation run field.
        existing_version: Fail the ingestion request if the  version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = "dayAheadFrontendContractModel"
    time: datetime.datetime
    title: str
    description: Optional[str] = None
    severity: Optional[str] = None
    alert_type: Optional[str] = Field(None, alias="alertType")
    status_code: Optional[int] = Field(None, alias="statusCode")
    event_ids: Optional[list[int]] = Field(None, alias="eventIds")
    calculation_run: Optional[str] = Field(None, alias="calculationRun")

    def _to_instances_apply(
        self, cache: set[str], view_by_write_class: dict[type[DomainModelApply], dm.ViewId] | None
    ) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))
        write_view = view_by_write_class and view_by_write_class.get(type(self))

        properties = {}
        if self.time is not None:
            properties["time"] = self.time.isoformat()
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
            source = dm.NodeOrEdgeData(
                source=write_view or dm.ViewId("dayAheadFrontendContractModel", "Alert", "1"),
                properties=properties,
            )
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[source],
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class AlertList(TypeList[Alert]):
    """List of alerts in read version."""

    _NODE = Alert

    def as_apply(self) -> AlertApplyList:
        """Convert this read version of alert to a write version."""
        return AlertApplyList([node.as_apply() for node in self.data])


class AlertApplyList(TypeApplyList[AlertApply]):
    """List of alerts in write version."""

    _NODE = AlertApply
