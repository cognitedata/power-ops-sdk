from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

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
    BooleanFilter,
    DateFilter,
    DirectRelationFilter,
    TimestampFilter,
)
from cognite.powerops.client._generated.v1.data_classes._bid_document import BidDocument, BidDocumentWrite
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._alert import Alert, AlertList, AlertGraphQL, AlertWrite, AlertWriteList
    from cognite.powerops.client._generated.v1.data_classes._bid_configuration_day_ahead import BidConfigurationDayAhead, BidConfigurationDayAheadList, BidConfigurationDayAheadGraphQL, BidConfigurationDayAheadWrite, BidConfigurationDayAheadWriteList
    from cognite.powerops.client._generated.v1.data_classes._bid_matrix_information import BidMatrixInformation, BidMatrixInformationList, BidMatrixInformationGraphQL, BidMatrixInformationWrite, BidMatrixInformationWriteList
    from cognite.powerops.client._generated.v1.data_classes._partial_bid_matrix_information import PartialBidMatrixInformation, PartialBidMatrixInformationList, PartialBidMatrixInformationGraphQL, PartialBidMatrixInformationWrite, PartialBidMatrixInformationWriteList


__all__ = [
    "BidDocumentDayAhead",
    "BidDocumentDayAheadWrite",
    "BidDocumentDayAheadList",
    "BidDocumentDayAheadWriteList",
    "BidDocumentDayAheadFields",
    "BidDocumentDayAheadTextFields",
    "BidDocumentDayAheadGraphQL",
]


BidDocumentDayAheadTextFields = Literal["external_id", "name", "workflow_execution_id"]
BidDocumentDayAheadFields = Literal["external_id", "name", "workflow_execution_id", "delivery_date", "start_calculation", "end_calculation", "is_complete"]

_BIDDOCUMENTDAYAHEAD_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
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
        name: Unique name for a given instance of a Bid Document. A combination of name, priceArea, date and
            startCalculation.
        workflow_execution_id: The process associated with the Bid calculation workflow.
        delivery_date: The date of the Bid.
        start_calculation: Timestamp of when the Bid calculation workflow started.
        end_calculation: Timestamp of when the Bid calculation workflow completed.
        is_complete: Indicates that the Bid calculation workflow has completed (although has not necessarily
            succeeded).
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

    def as_read(self) -> BidDocumentDayAhead:
        """Convert this GraphQL format of bid document day ahead to the reading format."""
        return BidDocumentDayAhead.model_validate(as_read_args(self))

    def as_write(self) -> BidDocumentDayAheadWrite:
        """Convert this GraphQL format of bid document day ahead to the writing format."""
        return BidDocumentDayAheadWrite.model_validate(as_write_args(self))


class BidDocumentDayAhead(BidDocument):
    """This represents the reading version of bid document day ahead.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid document day ahead.
        data_record: The data record of the bid document day ahead node.
        name: Unique name for a given instance of a Bid Document. A combination of name, priceArea, date and
            startCalculation.
        workflow_execution_id: The process associated with the Bid calculation workflow.
        delivery_date: The date of the Bid.
        start_calculation: Timestamp of when the Bid calculation workflow started.
        end_calculation: Timestamp of when the Bid calculation workflow completed.
        is_complete: Indicates that the Bid calculation workflow has completed (although has not necessarily
            succeeded).
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
    @field_validator("bid_configuration", "total", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("alerts", "partials", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> BidDocumentDayAheadWrite:
        """Convert this read version of bid document day ahead to the writing version."""
        return BidDocumentDayAheadWrite.model_validate(as_write_args(self))



class BidDocumentDayAheadWrite(BidDocumentWrite):
    """This represents the writing version of bid document day ahead.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid document day ahead.
        data_record: The data record of the bid document day ahead node.
        name: Unique name for a given instance of a Bid Document. A combination of name, priceArea, date and
            startCalculation.
        workflow_execution_id: The process associated with the Bid calculation workflow.
        delivery_date: The date of the Bid.
        start_calculation: Timestamp of when the Bid calculation workflow started.
        end_calculation: Timestamp of when the Bid calculation workflow completed.
        is_complete: Indicates that the Bid calculation workflow has completed (although has not necessarily
            succeeded).
        alerts: An array of calculation level Alerts.
        bid_configuration: The bid configuration field.
        total: The total field.
        partials: The partial field.
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("bid_configuration", "delivery_date", "end_calculation", "is_complete", "name", "start_calculation", "total", "workflow_execution_id",)
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (("alerts", dm.DirectRelationReference("power_ops_types", "calculationIssue")), ("partials", dm.DirectRelationReference("power_ops_types", "partialBid")),)
    _direct_relations: ClassVar[tuple[str, ...]] = ("bid_configuration", "total",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BidDocumentDayAhead", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "DayAheadBidDocument")
    bid_configuration: Union[BidConfigurationDayAheadWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="bidConfiguration")
    total: Union[BidMatrixInformationWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    partials: Optional[list[Union[PartialBidMatrixInformationWrite, str, dm.NodeId]]] = Field(default=None, repr=False)

    @field_validator("bid_configuration", "total", "partials", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class BidDocumentDayAheadList(DomainModelList[BidDocumentDayAhead]):
    """List of bid document day aheads in the read version."""

    _INSTANCE = BidDocumentDayAhead
    def as_write(self) -> BidDocumentDayAheadWriteList:
        """Convert these read versions of bid document day ahead to the writing versions."""
        return BidDocumentDayAheadWriteList([node.as_write() for node in self.data])


    @property
    def alerts(self) -> AlertList:
        from ._alert import Alert, AlertList
        return AlertList([item for items in self.data for item in items.alerts or [] if isinstance(item, Alert)])

    @property
    def bid_configuration(self) -> BidConfigurationDayAheadList:
        from ._bid_configuration_day_ahead import BidConfigurationDayAhead, BidConfigurationDayAheadList
        return BidConfigurationDayAheadList([item.bid_configuration for item in self.data if isinstance(item.bid_configuration, BidConfigurationDayAhead)])
    @property
    def total(self) -> BidMatrixInformationList:
        from ._bid_matrix_information import BidMatrixInformation, BidMatrixInformationList
        return BidMatrixInformationList([item.total for item in self.data if isinstance(item.total, BidMatrixInformation)])
    @property
    def partials(self) -> PartialBidMatrixInformationList:
        from ._partial_bid_matrix_information import PartialBidMatrixInformation, PartialBidMatrixInformationList
        return PartialBidMatrixInformationList([item for items in self.data for item in items.partials or [] if isinstance(item, PartialBidMatrixInformation)])


class BidDocumentDayAheadWriteList(DomainModelWriteList[BidDocumentDayAheadWrite]):
    """List of bid document day aheads in the writing version."""

    _INSTANCE = BidDocumentDayAheadWrite
    @property
    def alerts(self) -> AlertWriteList:
        from ._alert import AlertWrite, AlertWriteList
        return AlertWriteList([item for items in self.data for item in items.alerts or [] if isinstance(item, AlertWrite)])

    @property
    def bid_configuration(self) -> BidConfigurationDayAheadWriteList:
        from ._bid_configuration_day_ahead import BidConfigurationDayAheadWrite, BidConfigurationDayAheadWriteList
        return BidConfigurationDayAheadWriteList([item.bid_configuration for item in self.data if isinstance(item.bid_configuration, BidConfigurationDayAheadWrite)])
    @property
    def total(self) -> BidMatrixInformationWriteList:
        from ._bid_matrix_information import BidMatrixInformationWrite, BidMatrixInformationWriteList
        return BidMatrixInformationWriteList([item.total for item in self.data if isinstance(item.total, BidMatrixInformationWrite)])
    @property
    def partials(self) -> PartialBidMatrixInformationWriteList:
        from ._partial_bid_matrix_information import PartialBidMatrixInformationWrite, PartialBidMatrixInformationWriteList
        return PartialBidMatrixInformationWriteList([item for items in self.data for item in items.partials or [] if isinstance(item, PartialBidMatrixInformationWrite)])



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
    bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    total: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
    if isinstance(bid_configuration, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(bid_configuration):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bidConfiguration"), value=as_instance_dict_id(bid_configuration)))
    if bid_configuration and isinstance(bid_configuration, Sequence) and not isinstance(bid_configuration, str) and not is_tuple_id(bid_configuration):
        filters.append(dm.filters.In(view_id.as_property_ref("bidConfiguration"), values=[as_instance_dict_id(item) for item in bid_configuration]))
    if isinstance(total, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(total):
        filters.append(dm.filters.Equals(view_id.as_property_ref("total"), value=as_instance_dict_id(total)))
    if total and isinstance(total, Sequence) and not isinstance(total, str) and not is_tuple_id(total):
        filters.append(dm.filters.In(view_id.as_property_ref("total"), values=[as_instance_dict_id(item) for item in total]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _BidDocumentDayAheadQuery(NodeQueryCore[T_DomainModelList, BidDocumentDayAheadList]):
    _view_id = BidDocumentDayAhead._view_id
    _result_cls = BidDocumentDayAhead
    _result_list_cls_end = BidDocumentDayAheadList

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
        from ._alert import _AlertQuery
        from ._bid_configuration_day_ahead import _BidConfigurationDayAheadQuery
        from ._bid_matrix_information import _BidMatrixInformationQuery
        from ._partial_bid_matrix_information import _PartialBidMatrixInformationQuery

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

        if _AlertQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.alerts = _AlertQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="alerts",
                connection_property=ViewPropertyId(self._view_id, "alerts"),
            )

        if _BidConfigurationDayAheadQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.bid_configuration = _BidConfigurationDayAheadQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("bidConfiguration"),
                    direction="outwards",
                ),
                connection_name="bid_configuration",
                connection_property=ViewPropertyId(self._view_id, "bidConfiguration"),
            )

        if _BidMatrixInformationQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.total = _BidMatrixInformationQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("total"),
                    direction="outwards",
                ),
                connection_name="total",
                connection_property=ViewPropertyId(self._view_id, "total"),
            )

        if _PartialBidMatrixInformationQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.partials = _PartialBidMatrixInformationQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="partials",
                connection_property=ViewPropertyId(self._view_id, "partials"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.workflow_execution_id = StringFilter(self, self._view_id.as_property_ref("workflowExecutionId"))
        self.delivery_date = DateFilter(self, self._view_id.as_property_ref("deliveryDate"))
        self.start_calculation = TimestampFilter(self, self._view_id.as_property_ref("startCalculation"))
        self.end_calculation = TimestampFilter(self, self._view_id.as_property_ref("endCalculation"))
        self.is_complete = BooleanFilter(self, self._view_id.as_property_ref("isComplete"))
        self.bid_configuration_filter = DirectRelationFilter(self, self._view_id.as_property_ref("bidConfiguration"))
        self.total_filter = DirectRelationFilter(self, self._view_id.as_property_ref("total"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.workflow_execution_id,
            self.delivery_date,
            self.start_calculation,
            self.end_calculation,
            self.is_complete,
            self.bid_configuration_filter,
            self.total_filter,
        ])

    def list_bid_document_day_ahead(self, limit: int = DEFAULT_QUERY_LIMIT) -> BidDocumentDayAheadList:
        return self._list(limit=limit)


class BidDocumentDayAheadQuery(_BidDocumentDayAheadQuery[BidDocumentDayAheadList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, BidDocumentDayAheadList)
