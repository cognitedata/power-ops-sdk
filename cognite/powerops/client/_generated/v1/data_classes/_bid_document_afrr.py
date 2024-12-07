from __future__ import annotations

import datetime
import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal,  no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from pydantic import Field
from pydantic import field_validator, model_validator

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
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    is_tuple_id,
    select_best_node,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    BooleanFilter,
    DateFilter,
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
    "BidDocumentAFRRApply",
    "BidDocumentAFRRList",
    "BidDocumentAFRRWriteList",
    "BidDocumentAFRRApplyList",
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
        name: Unique name for a given instance of a Bid Document. A combination of name, priceArea, date and startCalculation.
        workflow_execution_id: The process associated with the Bid calculation workflow.
        delivery_date: The date of the Bid.
        start_calculation: Timestamp of when the Bid calculation workflow started.
        end_calculation: Timestamp of when the Bid calculation workflow completed.
        is_complete: Indicates that the Bid calculation workflow has completed (although has not necessarily succeeded).
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> BidDocumentAFRR:
        """Convert this GraphQL format of bid document afrr to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return BidDocumentAFRR(
            space=self.space,
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
            alerts=[alert.as_read() for alert in self.alerts] if self.alerts is not None else None,
            price_area=self.price_area.as_read()
if isinstance(self.price_area, GraphQLCore)
else self.price_area,
            bids=[bid.as_read() for bid in self.bids] if self.bids is not None else None,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BidDocumentAFRRWrite:
        """Convert this GraphQL format of bid document afrr to the writing format."""
        return BidDocumentAFRRWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            workflow_execution_id=self.workflow_execution_id,
            delivery_date=self.delivery_date,
            start_calculation=self.start_calculation,
            end_calculation=self.end_calculation,
            is_complete=self.is_complete,
            alerts=[alert.as_write() for alert in self.alerts] if self.alerts is not None else None,
            price_area=self.price_area.as_write()
if isinstance(self.price_area, GraphQLCore)
else self.price_area,
            bids=[bid.as_write() for bid in self.bids] if self.bids is not None else None,
        )


class BidDocumentAFRR(BidDocument):
    """This represents the reading version of bid document afrr.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid document afrr.
        data_record: The data record of the bid document afrr node.
        name: Unique name for a given instance of a Bid Document. A combination of name, priceArea, date and startCalculation.
        workflow_execution_id: The process associated with the Bid calculation workflow.
        delivery_date: The date of the Bid.
        start_calculation: Timestamp of when the Bid calculation workflow started.
        end_calculation: Timestamp of when the Bid calculation workflow completed.
        is_complete: Indicates that the Bid calculation workflow has completed (although has not necessarily succeeded).
        alerts: An array of calculation level Alerts.
        price_area: The price area field.
        bids: An array of BidRows containing the Bid data.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BidDocumentAFRR", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "AFRRBidDocument")
    price_area: Union[PriceAreaAFRR, str, dm.NodeId, None] = Field(default=None, repr=False, alias="priceArea")
    bids: Optional[list[Union[BidRow, str, dm.NodeId]]] = Field(default=None, repr=False)

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BidDocumentAFRRWrite:
        """Convert this read version of bid document afrr to the writing version."""
        return BidDocumentAFRRWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            workflow_execution_id=self.workflow_execution_id,
            delivery_date=self.delivery_date,
            start_calculation=self.start_calculation,
            end_calculation=self.end_calculation,
            is_complete=self.is_complete,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts] if self.alerts is not None else None,
            price_area=self.price_area.as_write()
if isinstance(self.price_area, DomainModel)
else self.price_area,
            bids=[bid.as_write() if isinstance(bid, DomainModel) else bid for bid in self.bids] if self.bids is not None else None,
        )

    def as_apply(self) -> BidDocumentAFRRWrite:
        """Convert this read version of bid document afrr to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()
    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, BidDocumentAFRR],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._alert import Alert
        from ._bid_row import BidRow
        from ._price_area_afrr import PriceAreaAFRR
        for instance in instances.values():
            if isinstance(instance.price_area, (dm.NodeId, str)) and (price_area := nodes_by_id.get(instance.price_area)) and isinstance(
                    price_area, PriceAreaAFRR
            ):
                instance.price_area = price_area
            if edges := edges_by_source_node.get(instance.as_id()):
                alerts: list[Alert | str | dm.NodeId] = []
                bids: list[BidRow | str | dm.NodeId] = []
                for edge in edges:
                    value: DomainModel | DomainRelation | str | dm.NodeId
                    if isinstance(edge, DomainRelation):
                        value = edge
                    else:
                        other_end: dm.DirectRelationReference = (
                            edge.end_node
                            if edge.start_node.space == instance.space
                            and edge.start_node.external_id == instance.external_id
                            else edge.start_node
                        )
                        destination: dm.NodeId | str = (
                            as_node_id(other_end)
                            if other_end.space != DEFAULT_INSTANCE_SPACE
                            else other_end.external_id
                        )
                        if destination in nodes_by_id:
                            value = nodes_by_id[destination]
                        else:
                            value = destination
                    edge_type = edge.edge_type if isinstance(edge, DomainRelation) else edge.type

                    if edge_type == dm.DirectRelationReference("power_ops_types", "calculationIssue") and isinstance(
                        value, (Alert, str, dm.NodeId)
                    ):
                        alerts.append(value)
                    if edge_type == dm.DirectRelationReference("power_ops_types", "partialBid") and isinstance(
                        value, (BidRow, str, dm.NodeId)
                    ):
                        bids.append(value)

                instance.alerts = alerts or None
                instance.bids = bids or None



class BidDocumentAFRRWrite(BidDocumentWrite):
    """This represents the writing version of bid document afrr.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid document afrr.
        data_record: The data record of the bid document afrr node.
        name: Unique name for a given instance of a Bid Document. A combination of name, priceArea, date and startCalculation.
        workflow_execution_id: The process associated with the Bid calculation workflow.
        delivery_date: The date of the Bid.
        start_calculation: Timestamp of when the Bid calculation workflow started.
        end_calculation: Timestamp of when the Bid calculation workflow completed.
        is_complete: Indicates that the Bid calculation workflow has completed (although has not necessarily succeeded).
        alerts: An array of calculation level Alerts.
        price_area: The price area field.
        bids: An array of BidRows containing the Bid data.
    """

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

        if self.price_area is not None:
            properties["priceArea"] = {
                "space":  self.space if isinstance(self.price_area, str) else self.price_area.space,
                "externalId": self.price_area if isinstance(self.price_area, str) else self.price_area.external_id,
            }

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=as_direct_relation_reference(self.node_type),
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
        for bid in self.bids or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=bid,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.price_area, DomainModelWrite):
            other_resources = self.price_area._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class BidDocumentAFRRApply(BidDocumentAFRRWrite):
    def __new__(cls, *args, **kwargs) -> BidDocumentAFRRApply:
        warnings.warn(
            "BidDocumentAFRRApply is deprecated and will be removed in v1.0. Use BidDocumentAFRRWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "BidDocumentAFRR.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)

class BidDocumentAFRRList(DomainModelList[BidDocumentAFRR]):
    """List of bid document afrrs in the read version."""

    _INSTANCE = BidDocumentAFRR
    def as_write(self) -> BidDocumentAFRRWriteList:
        """Convert these read versions of bid document afrr to the writing versions."""
        return BidDocumentAFRRWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> BidDocumentAFRRWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

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


class BidDocumentAFRRApplyList(BidDocumentAFRRWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
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
            connection_type,
            reverse_expression,
        )

        if _AlertQuery not in created_types:
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
            )

        if _PriceAreaAFRRQuery not in created_types:
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
            )

        if _BidRowQuery not in created_types:
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
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.workflow_execution_id = StringFilter(self, self._view_id.as_property_ref("workflowExecutionId"))
        self.delivery_date = DateFilter(self, self._view_id.as_property_ref("deliveryDate"))
        self.start_calculation = TimestampFilter(self, self._view_id.as_property_ref("startCalculation"))
        self.end_calculation = TimestampFilter(self, self._view_id.as_property_ref("endCalculation"))
        self.is_complete = BooleanFilter(self, self._view_id.as_property_ref("isComplete"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.workflow_execution_id,
            self.delivery_date,
            self.start_calculation,
            self.end_calculation,
            self.is_complete,
        ])

    def list_bid_document_afrr(self, limit: int = DEFAULT_QUERY_LIMIT) -> BidDocumentAFRRList:
        return self._list(limit=limit)


class BidDocumentAFRRQuery(_BidDocumentAFRRQuery[BidDocumentAFRRList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, BidDocumentAFRRList)
