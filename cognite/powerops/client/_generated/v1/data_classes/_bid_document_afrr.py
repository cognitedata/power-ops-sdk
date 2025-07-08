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
    from cognite.powerops.client._generated.v1.data_classes._bid_row import BidRow, BidRowList, BidRowGraphQL, BidRowWrite, BidRowWriteList
    from cognite.powerops.client._generated.v1.data_classes._price_area_afrr import PriceAreaAFRR, PriceAreaAFRRList, PriceAreaAFRRGraphQL, PriceAreaAFRRWrite, PriceAreaAFRRWriteList


__all__ = [
    "BidDocumentAFRR",
    "BidDocumentAFRRWrite",
    "BidDocumentAFRRList",
    "BidDocumentAFRRWriteList",
    "BidDocumentAFRRFields",
    "BidDocumentAFRRTextFields",
    "BidDocumentAFRRGraphQL",
]


BidDocumentAFRRTextFields = Literal["external_id", "name", "workflow_execution_id"]
BidDocumentAFRRFields = Literal["external_id", "name", "workflow_execution_id", "delivery_date", "start_calculation", "end_calculation", "is_complete"]

_BIDDOCUMENTAFRR_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
    "workflow_execution_id": "workflowExecutionId",
    "delivery_date": "deliveryDate",
    "start_calculation": "startCalculation",
    "end_calculation": "endCalculation",
    "is_complete": "isComplete",
}


class BidDocumentAFRRGraphQL(GraphQLCore):
    """This represents the reading version of bid document afrr, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid document afrr.
        data_record: The data record of the bid document afrr node.
        name: Unique name for a given instance of a Bid Document. A combination of name, priceArea, date and
            startCalculation.
        workflow_execution_id: The process associated with the Bid calculation workflow.
        delivery_date: The date of the Bid.
        start_calculation: Timestamp of when the Bid calculation workflow started.
        end_calculation: Timestamp of when the Bid calculation workflow completed.
        is_complete: Indicates that the Bid calculation workflow has completed (although has not necessarily
            succeeded).
        alerts: An array of calculation level Alerts.
        price_area: The price area field.
        bids: An array of BidRows containing the Bid data.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BidDocumentAFRR", "1")
    name: Optional[str] = None
    workflow_execution_id: Optional[str] = Field(None, alias="workflowExecutionId")
    delivery_date: Optional[datetime.date] = Field(None, alias="deliveryDate")
    start_calculation: Optional[datetime.datetime] = Field(None, alias="startCalculation")
    end_calculation: Optional[datetime.datetime] = Field(None, alias="endCalculation")
    is_complete: Optional[bool] = Field(None, alias="isComplete")
    alerts: Optional[list[AlertGraphQL]] = Field(default=None, repr=False)
    price_area: Optional[PriceAreaAFRRGraphQL] = Field(default=None, repr=False, alias="priceArea")
    bids: Optional[list[BidRowGraphQL]] = Field(default=None, repr=False)

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


    @field_validator("alerts", "price_area", "bids", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> BidDocumentAFRR:
        """Convert this GraphQL format of bid document afrr to the reading format."""
        return BidDocumentAFRR.model_validate(as_read_args(self))

    def as_write(self) -> BidDocumentAFRRWrite:
        """Convert this GraphQL format of bid document afrr to the writing format."""
        return BidDocumentAFRRWrite.model_validate(as_write_args(self))


class BidDocumentAFRR(BidDocument):
    """This represents the reading version of bid document afrr.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid document afrr.
        data_record: The data record of the bid document afrr node.
        name: Unique name for a given instance of a Bid Document. A combination of name, priceArea, date and
            startCalculation.
        workflow_execution_id: The process associated with the Bid calculation workflow.
        delivery_date: The date of the Bid.
        start_calculation: Timestamp of when the Bid calculation workflow started.
        end_calculation: Timestamp of when the Bid calculation workflow completed.
        is_complete: Indicates that the Bid calculation workflow has completed (although has not necessarily
            succeeded).
        alerts: An array of calculation level Alerts.
        price_area: The price area field.
        bids: An array of BidRows containing the Bid data.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BidDocumentAFRR", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "AFRRBidDocument")
    price_area: Union[PriceAreaAFRR, str, dm.NodeId, None] = Field(default=None, repr=False, alias="priceArea")
    bids: Optional[list[Union[BidRow, str, dm.NodeId]]] = Field(default=None, repr=False)
    @field_validator("price_area", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("alerts", "bids", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> BidDocumentAFRRWrite:
        """Convert this read version of bid document afrr to the writing version."""
        return BidDocumentAFRRWrite.model_validate(as_write_args(self))



class BidDocumentAFRRWrite(BidDocumentWrite):
    """This represents the writing version of bid document afrr.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid document afrr.
        data_record: The data record of the bid document afrr node.
        name: Unique name for a given instance of a Bid Document. A combination of name, priceArea, date and
            startCalculation.
        workflow_execution_id: The process associated with the Bid calculation workflow.
        delivery_date: The date of the Bid.
        start_calculation: Timestamp of when the Bid calculation workflow started.
        end_calculation: Timestamp of when the Bid calculation workflow completed.
        is_complete: Indicates that the Bid calculation workflow has completed (although has not necessarily
            succeeded).
        alerts: An array of calculation level Alerts.
        price_area: The price area field.
        bids: An array of BidRows containing the Bid data.
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("delivery_date", "end_calculation", "is_complete", "name", "price_area", "start_calculation", "workflow_execution_id",)
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (("alerts", dm.DirectRelationReference("power_ops_types", "calculationIssue")), ("bids", dm.DirectRelationReference("power_ops_types", "partialBid")),)
    _direct_relations: ClassVar[tuple[str, ...]] = ("price_area",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BidDocumentAFRR", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "AFRRBidDocument")
    price_area: Union[PriceAreaAFRRWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="priceArea")
    bids: Optional[list[Union[BidRowWrite, str, dm.NodeId]]] = Field(default=None, repr=False)

    @field_validator("price_area", "bids", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class BidDocumentAFRRList(DomainModelList[BidDocumentAFRR]):
    """List of bid document afrrs in the read version."""

    _INSTANCE = BidDocumentAFRR
    def as_write(self) -> BidDocumentAFRRWriteList:
        """Convert these read versions of bid document afrr to the writing versions."""
        return BidDocumentAFRRWriteList([node.as_write() for node in self.data])


    @property
    def alerts(self) -> AlertList:
        from ._alert import Alert, AlertList
        return AlertList([item for items in self.data for item in items.alerts or [] if isinstance(item, Alert)])

    @property
    def price_area(self) -> PriceAreaAFRRList:
        from ._price_area_afrr import PriceAreaAFRR, PriceAreaAFRRList
        return PriceAreaAFRRList([item.price_area for item in self.data if isinstance(item.price_area, PriceAreaAFRR)])
    @property
    def bids(self) -> BidRowList:
        from ._bid_row import BidRow, BidRowList
        return BidRowList([item for items in self.data for item in items.bids or [] if isinstance(item, BidRow)])


class BidDocumentAFRRWriteList(DomainModelWriteList[BidDocumentAFRRWrite]):
    """List of bid document afrrs in the writing version."""

    _INSTANCE = BidDocumentAFRRWrite
    @property
    def alerts(self) -> AlertWriteList:
        from ._alert import AlertWrite, AlertWriteList
        return AlertWriteList([item for items in self.data for item in items.alerts or [] if isinstance(item, AlertWrite)])

    @property
    def price_area(self) -> PriceAreaAFRRWriteList:
        from ._price_area_afrr import PriceAreaAFRRWrite, PriceAreaAFRRWriteList
        return PriceAreaAFRRWriteList([item.price_area for item in self.data if isinstance(item.price_area, PriceAreaAFRRWrite)])
    @property
    def bids(self) -> BidRowWriteList:
        from ._bid_row import BidRowWrite, BidRowWriteList
        return BidRowWriteList([item for items in self.data for item in items.bids or [] if isinstance(item, BidRowWrite)])



def _create_bid_document_afrr_filter(
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
    price_area: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
    if isinstance(price_area, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(price_area):
        filters.append(dm.filters.Equals(view_id.as_property_ref("priceArea"), value=as_instance_dict_id(price_area)))
    if price_area and isinstance(price_area, Sequence) and not isinstance(price_area, str) and not is_tuple_id(price_area):
        filters.append(dm.filters.In(view_id.as_property_ref("priceArea"), values=[as_instance_dict_id(item) for item in price_area]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _BidDocumentAFRRQuery(NodeQueryCore[T_DomainModelList, BidDocumentAFRRList]):
    _view_id = BidDocumentAFRR._view_id
    _result_cls = BidDocumentAFRR
    _result_list_cls_end = BidDocumentAFRRList

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
        from ._bid_row import _BidRowQuery
        from ._price_area_afrr import _PriceAreaAFRRQuery

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

        if _PriceAreaAFRRQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.price_area = _PriceAreaAFRRQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("priceArea"),
                    direction="outwards",
                ),
                connection_name="price_area",
                connection_property=ViewPropertyId(self._view_id, "priceArea"),
            )

        if _BidRowQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.bids = _BidRowQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="bids",
                connection_property=ViewPropertyId(self._view_id, "bids"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.workflow_execution_id = StringFilter(self, self._view_id.as_property_ref("workflowExecutionId"))
        self.delivery_date = DateFilter(self, self._view_id.as_property_ref("deliveryDate"))
        self.start_calculation = TimestampFilter(self, self._view_id.as_property_ref("startCalculation"))
        self.end_calculation = TimestampFilter(self, self._view_id.as_property_ref("endCalculation"))
        self.is_complete = BooleanFilter(self, self._view_id.as_property_ref("isComplete"))
        self.price_area_filter = DirectRelationFilter(self, self._view_id.as_property_ref("priceArea"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.workflow_execution_id,
            self.delivery_date,
            self.start_calculation,
            self.end_calculation,
            self.is_complete,
            self.price_area_filter,
        ])

    def list_bid_document_afrr(self, limit: int = DEFAULT_QUERY_LIMIT) -> BidDocumentAFRRList:
        return self._list(limit=limit)


class BidDocumentAFRRQuery(_BidDocumentAFRRQuery[BidDocumentAFRRList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, BidDocumentAFRRList)
