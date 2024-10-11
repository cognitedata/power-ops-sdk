from __future__ import annotations

import datetime
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
from ._bid_document import BidDocument, BidDocumentWrite

if TYPE_CHECKING:
    from ._alert import Alert, AlertGraphQL, AlertWrite
    from ._bid_configuration_day_ahead import BidConfigurationDayAhead, BidConfigurationDayAheadGraphQL, BidConfigurationDayAheadWrite
    from ._bid_matrix_information import BidMatrixInformation, BidMatrixInformationGraphQL, BidMatrixInformationWrite
    from ._partial_bid_matrix_information import PartialBidMatrixInformation, PartialBidMatrixInformationGraphQL, PartialBidMatrixInformationWrite


__all__ = [
    "BidDocumentDayAhead",
    "BidDocumentDayAheadWrite",
    "BidDocumentDayAheadApply",
    "BidDocumentDayAheadList",
    "BidDocumentDayAheadWriteList",
    "BidDocumentDayAheadApplyList",
    "BidDocumentDayAheadFields",
    "BidDocumentDayAheadTextFields",
    "BidDocumentDayAheadGraphQL",
]


BidDocumentDayAheadTextFields = Literal["name", "workflow_execution_id"]
BidDocumentDayAheadFields = Literal["name", "workflow_execution_id", "delivery_date", "start_calculation", "end_calculation", "is_complete"]

_BIDDOCUMENTDAYAHEAD_PROPERTIES_BY_FIELD = {
    "name": "name",
    "workflow_execution_id": "workflowExecutionId",
    "delivery_date": "deliveryDate",
    "start_calculation": "startCalculation",
    "end_calculation": "endCalculation",
    "is_complete": "isComplete",
}

class BidDocumentDayAheadGraphQL(GraphQLCore):
    """This represents the reading version of bid document day ahead, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid document day ahead.
        data_record: The data record of the bid document day ahead node.
        name: Unique name for a given instance of a Bid Document. A combination of name, priceArea, date and startCalculation.
        workflow_execution_id: The process associated with the Bid calculation workflow.
        delivery_date: The date of the Bid.
        start_calculation: Timestamp of when the Bid calculation workflow started.
        end_calculation: Timestamp of when the Bid calculation workflow completed.
        is_complete: Indicates that the Bid calculation workflow has completed (although has not necessarily succeeded).
        alerts: An array of calculation level Alerts.
        bid_configuration: The bid configuration field.
        total: The total field.
        partials: The partial field.
    """
    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BidDocumentDayAhead", "1")
    name: Optional[str] = None
    workflow_execution_id: Optional[str] = Field(None, alias="workflowExecutionId")
    delivery_date: Optional[datetime.date] = Field(None, alias="deliveryDate")
    start_calculation: Optional[datetime.datetime] = Field(None, alias="startCalculation")
    end_calculation: Optional[datetime.datetime] = Field(None, alias="endCalculation")
    is_complete: Optional[bool] = Field(None, alias="isComplete")
    alerts: Optional[list[AlertGraphQL]] = Field(default=None, repr=False)
    bid_configuration: Optional[BidConfigurationDayAheadGraphQL] = Field(default=None, repr=False, alias="bidConfiguration")
    total: Optional[BidMatrixInformationGraphQL] = Field(default=None, repr=False)
    partials: Optional[list[PartialBidMatrixInformationGraphQL]] = Field(default=None, repr=False)

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
    @field_validator("alerts", "bid_configuration", "total", "partials", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> BidDocumentDayAhead:
        """Convert this GraphQL format of bid document day ahead to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return BidDocumentDayAhead(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            workflow_execution_id=self.workflow_execution_id,
            delivery_date=self.delivery_date,
            start_calculation=self.start_calculation,
            end_calculation=self.end_calculation,
            is_complete=self.is_complete,
            alerts=[alert.as_read() for alert in self.alerts or []],
            bid_configuration=self.bid_configuration.as_read() if isinstance(self.bid_configuration, GraphQLCore) else self.bid_configuration,
            total=self.total.as_read() if isinstance(self.total, GraphQLCore) else self.total,
            partials=[partial.as_read() for partial in self.partials or []],
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BidDocumentDayAheadWrite:
        """Convert this GraphQL format of bid document day ahead to the writing format."""
        return BidDocumentDayAheadWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            workflow_execution_id=self.workflow_execution_id,
            delivery_date=self.delivery_date,
            start_calculation=self.start_calculation,
            end_calculation=self.end_calculation,
            is_complete=self.is_complete,
            alerts=[alert.as_write() for alert in self.alerts or []],
            bid_configuration=self.bid_configuration.as_write() if isinstance(self.bid_configuration, GraphQLCore) else self.bid_configuration,
            total=self.total.as_write() if isinstance(self.total, GraphQLCore) else self.total,
            partials=[partial.as_write() for partial in self.partials or []],
        )


class BidDocumentDayAhead(BidDocument):
    """This represents the reading version of bid document day ahead.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid document day ahead.
        data_record: The data record of the bid document day ahead node.
        name: Unique name for a given instance of a Bid Document. A combination of name, priceArea, date and startCalculation.
        workflow_execution_id: The process associated with the Bid calculation workflow.
        delivery_date: The date of the Bid.
        start_calculation: Timestamp of when the Bid calculation workflow started.
        end_calculation: Timestamp of when the Bid calculation workflow completed.
        is_complete: Indicates that the Bid calculation workflow has completed (although has not necessarily succeeded).
        alerts: An array of calculation level Alerts.
        bid_configuration: The bid configuration field.
        total: The total field.
        partials: The partial field.
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BidDocumentDayAhead", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "DayAheadBidDocument")
    bid_configuration: Union[BidConfigurationDayAhead, str, dm.NodeId, None] = Field(default=None, repr=False, alias="bidConfiguration")
    total: Union[BidMatrixInformation, str, dm.NodeId, None] = Field(default=None, repr=False)
    partials: Optional[list[Union[PartialBidMatrixInformation, str, dm.NodeId]]] = Field(default=None, repr=False)

    def as_write(self) -> BidDocumentDayAheadWrite:
        """Convert this read version of bid document day ahead to the writing version."""
        return BidDocumentDayAheadWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            workflow_execution_id=self.workflow_execution_id,
            delivery_date=self.delivery_date,
            start_calculation=self.start_calculation,
            end_calculation=self.end_calculation,
            is_complete=self.is_complete,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
            bid_configuration=self.bid_configuration.as_write() if isinstance(self.bid_configuration, DomainModel) else self.bid_configuration,
            total=self.total.as_write() if isinstance(self.total, DomainModel) else self.total,
            partials=[partial.as_write() if isinstance(partial, DomainModel) else partial for partial in self.partials or []],
        )

    def as_apply(self) -> BidDocumentDayAheadWrite:
        """Convert this read version of bid document day ahead to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidDocumentDayAheadWrite(BidDocumentWrite):
    """This represents the writing version of bid document day ahead.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid document day ahead.
        data_record: The data record of the bid document day ahead node.
        name: Unique name for a given instance of a Bid Document. A combination of name, priceArea, date and startCalculation.
        workflow_execution_id: The process associated with the Bid calculation workflow.
        delivery_date: The date of the Bid.
        start_calculation: Timestamp of when the Bid calculation workflow started.
        end_calculation: Timestamp of when the Bid calculation workflow completed.
        is_complete: Indicates that the Bid calculation workflow has completed (although has not necessarily succeeded).
        alerts: An array of calculation level Alerts.
        bid_configuration: The bid configuration field.
        total: The total field.
        partials: The partial field.
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BidDocumentDayAhead", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "DayAheadBidDocument")
    bid_configuration: Union[BidConfigurationDayAheadWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="bidConfiguration")
    total: Union[BidMatrixInformationWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    partials: Optional[list[Union[PartialBidMatrixInformationWrite, str, dm.NodeId]]] = Field(default=None, repr=False)

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

        if self.name is not None or write_none:
            properties["name"] = self.name

        if self.workflow_execution_id is not None or write_none:
            properties["workflowExecutionId"] = self.workflow_execution_id

        if self.delivery_date is not None:
            properties["deliveryDate"] = self.delivery_date.isoformat() if self.delivery_date else None

        if self.start_calculation is not None or write_none:
            properties["startCalculation"] = self.start_calculation.isoformat(timespec="milliseconds") if self.start_calculation else None

        if self.end_calculation is not None or write_none:
            properties["endCalculation"] = self.end_calculation.isoformat(timespec="milliseconds") if self.end_calculation else None

        if self.is_complete is not None or write_none:
            properties["isComplete"] = self.is_complete

        if self.bid_configuration is not None:
            properties["bidConfiguration"] = {
                "space":  self.space if isinstance(self.bid_configuration, str) else self.bid_configuration.space,
                "externalId": self.bid_configuration if isinstance(self.bid_configuration, str) else self.bid_configuration.external_id,
            }

        if self.total is not None:
            properties["total"] = {
                "space":  self.space if isinstance(self.total, str) else self.total.space,
                "externalId": self.total if isinstance(self.total, str) else self.total.external_id,
            }


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

        edge_type = dm.DirectRelationReference("power_ops_types", "partialBid")
        for partial in self.partials or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=partial,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.bid_configuration, DomainModelWrite):
            other_resources = self.bid_configuration._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.total, DomainModelWrite):
            other_resources = self.total._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class BidDocumentDayAheadApply(BidDocumentDayAheadWrite):
    def __new__(cls, *args, **kwargs) -> BidDocumentDayAheadApply:
        warnings.warn(
            "BidDocumentDayAheadApply is deprecated and will be removed in v1.0. Use BidDocumentDayAheadWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "BidDocumentDayAhead.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class BidDocumentDayAheadList(DomainModelList[BidDocumentDayAhead]):
    """List of bid document day aheads in the read version."""

    _INSTANCE = BidDocumentDayAhead

    def as_write(self) -> BidDocumentDayAheadWriteList:
        """Convert these read versions of bid document day ahead to the writing versions."""
        return BidDocumentDayAheadWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> BidDocumentDayAheadWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BidDocumentDayAheadWriteList(DomainModelWriteList[BidDocumentDayAheadWrite]):
    """List of bid document day aheads in the writing version."""

    _INSTANCE = BidDocumentDayAheadWrite

class BidDocumentDayAheadApplyList(BidDocumentDayAheadWriteList): ...



def _create_bid_document_day_ahead_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    workflow_execution_id: str | list[str] | None = None,
    workflow_execution_id_prefix: str | None = None,
    min_delivery_date: datetime.date | None = None,
    max_delivery_date: datetime.date | None = None,
    min_start_calculation: datetime.datetime | None = None,
    max_start_calculation: datetime.datetime | None = None,
    min_end_calculation: datetime.datetime | None = None,
    max_end_calculation: datetime.datetime | None = None,
    is_complete: bool | None = None,
    bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if isinstance(workflow_execution_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("workflowExecutionId"), value=workflow_execution_id))
    if workflow_execution_id and isinstance(workflow_execution_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("workflowExecutionId"), values=workflow_execution_id))
    if workflow_execution_id_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("workflowExecutionId"), value=workflow_execution_id_prefix))
    if min_delivery_date is not None or max_delivery_date is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("deliveryDate"), gte=min_delivery_date.isoformat() if min_delivery_date else None, lte=max_delivery_date.isoformat() if max_delivery_date else None))
    if min_start_calculation is not None or max_start_calculation is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("startCalculation"), gte=min_start_calculation.isoformat(timespec="milliseconds") if min_start_calculation else None, lte=max_start_calculation.isoformat(timespec="milliseconds") if max_start_calculation else None))
    if min_end_calculation is not None or max_end_calculation is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("endCalculation"), gte=min_end_calculation.isoformat(timespec="milliseconds") if min_end_calculation else None, lte=max_end_calculation.isoformat(timespec="milliseconds") if max_end_calculation else None))
    if isinstance(is_complete, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isComplete"), value=is_complete))
    if bid_configuration and isinstance(bid_configuration, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bidConfiguration"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": bid_configuration}))
    if bid_configuration and isinstance(bid_configuration, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bidConfiguration"), value={"space": bid_configuration[0], "externalId": bid_configuration[1]}))
    if bid_configuration and isinstance(bid_configuration, list) and isinstance(bid_configuration[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("bidConfiguration"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in bid_configuration]))
    if bid_configuration and isinstance(bid_configuration, list) and isinstance(bid_configuration[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("bidConfiguration"), values=[{"space": item[0], "externalId": item[1]} for item in bid_configuration]))
    if total and isinstance(total, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("total"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": total}))
    if total and isinstance(total, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("total"), value={"space": total[0], "externalId": total[1]}))
    if total and isinstance(total, list) and isinstance(total[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("total"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in total]))
    if total and isinstance(total, list) and isinstance(total[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("total"), values=[{"space": item[0], "externalId": item[1]} for item in total]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
