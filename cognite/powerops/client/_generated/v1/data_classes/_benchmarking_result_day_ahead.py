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
    FloatFilter,
    TimestampFilter,
)
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._alert import Alert, AlertList, AlertGraphQL, AlertWrite, AlertWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_result import ShopResult, ShopResultList, ShopResultGraphQL, ShopResultWrite, ShopResultWriteList


__all__ = [
    "BenchmarkingResultDayAhead",
    "BenchmarkingResultDayAheadWrite",
    "BenchmarkingResultDayAheadList",
    "BenchmarkingResultDayAheadWriteList",
    "BenchmarkingResultDayAheadFields",
    "BenchmarkingResultDayAheadTextFields",
    "BenchmarkingResultDayAheadGraphQL",
]


BenchmarkingResultDayAheadTextFields = Literal["external_id", "name", "workflow_execution_id"]
BenchmarkingResultDayAheadFields = Literal["external_id", "name", "workflow_execution_id", "delivery_date", "bid_generated", "is_selected", "value"]

_BENCHMARKINGRESULTDAYAHEAD_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
    "workflow_execution_id": "workflowExecutionId",
    "delivery_date": "deliveryDate",
    "bid_generated": "bidGenerated",
    "is_selected": "isSelected",
    "value": "value",
}


class BenchmarkingResultDayAheadGraphQL(GraphQLCore):
    """This represents the reading version of benchmarking result day ahead, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the benchmarking result day ahead.
        data_record: The data record of the benchmarking result day ahead node.
        name: Unique name for a given instance of a Benchmarking result.
        workflow_execution_id: The process associated with the benchmarking workflow.
        bid_source: The bid source field.
        delivery_date: The delivery date
        bid_generated: Timestamp of when the bid had been generated
        shop_result: The shop result field.
        is_selected: Indicating that this is the one result (for the bidSource and deliveryDate) that should be shown
            by default in the UI
        value: This would normally be the objective value ('grand total') from the Shop result, or maybe the difference
            between the objective value in this run and for 'upper bound', but it should be possible to override it
            (e. g. if the difference is above some limit)
        alerts: An array of benchmarking calculation level Alerts.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BenchmarkingResultDayAhead", "1")
    name: Optional[str] = None
    workflow_execution_id: Optional[str] = Field(None, alias="workflowExecutionId")
    bid_source: Optional[dict] = Field(default=None, alias="bidSource")
    delivery_date: Optional[datetime.date] = Field(None, alias="deliveryDate")
    bid_generated: Optional[datetime.datetime] = Field(None, alias="bidGenerated")
    shop_result: Optional[ShopResultGraphQL] = Field(default=None, repr=False, alias="shopResult")
    is_selected: Optional[bool] = Field(None, alias="isSelected")
    value: Optional[float] = None
    alerts: Optional[list[AlertGraphQL]] = Field(default=None, repr=False)

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


    @field_validator("bid_source", "shop_result", "alerts", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> BenchmarkingResultDayAhead:
        """Convert this GraphQL format of benchmarking result day ahead to the reading format."""
        return BenchmarkingResultDayAhead.model_validate(as_read_args(self))

    def as_write(self) -> BenchmarkingResultDayAheadWrite:
        """Convert this GraphQL format of benchmarking result day ahead to the writing format."""
        return BenchmarkingResultDayAheadWrite.model_validate(as_write_args(self))


class BenchmarkingResultDayAhead(DomainModel):
    """This represents the reading version of benchmarking result day ahead.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the benchmarking result day ahead.
        data_record: The data record of the benchmarking result day ahead node.
        name: Unique name for a given instance of a Benchmarking result.
        workflow_execution_id: The process associated with the benchmarking workflow.
        bid_source: The bid source field.
        delivery_date: The delivery date
        bid_generated: Timestamp of when the bid had been generated
        shop_result: The shop result field.
        is_selected: Indicating that this is the one result (for the bidSource and deliveryDate) that should be shown
            by default in the UI
        value: This would normally be the objective value ('grand total') from the Shop result, or maybe the difference
            between the objective value in this run and for 'upper bound', but it should be possible to override it
            (e. g. if the difference is above some limit)
        alerts: An array of benchmarking calculation level Alerts.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BenchmarkingResultDayAhead", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "BenchmarkingResultDayAhead")
    name: Optional[str] = None
    workflow_execution_id: Optional[str] = Field(None, alias="workflowExecutionId")
    bid_source: Union[str, dm.NodeId, None] = Field(default=None, alias="bidSource")
    delivery_date: Optional[datetime.date] = Field(None, alias="deliveryDate")
    bid_generated: Optional[datetime.datetime] = Field(None, alias="bidGenerated")
    shop_result: Union[ShopResult, str, dm.NodeId, None] = Field(default=None, repr=False, alias="shopResult")
    is_selected: Optional[bool] = Field(None, alias="isSelected")
    value: Optional[float] = None
    alerts: Optional[list[Union[Alert, str, dm.NodeId]]] = Field(default=None, repr=False)
    @field_validator("bid_source", "shop_result", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("alerts", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> BenchmarkingResultDayAheadWrite:
        """Convert this read version of benchmarking result day ahead to the writing version."""
        return BenchmarkingResultDayAheadWrite.model_validate(as_write_args(self))



class BenchmarkingResultDayAheadWrite(DomainModelWrite):
    """This represents the writing version of benchmarking result day ahead.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the benchmarking result day ahead.
        data_record: The data record of the benchmarking result day ahead node.
        name: Unique name for a given instance of a Benchmarking result.
        workflow_execution_id: The process associated with the benchmarking workflow.
        bid_source: The bid source field.
        delivery_date: The delivery date
        bid_generated: Timestamp of when the bid had been generated
        shop_result: The shop result field.
        is_selected: Indicating that this is the one result (for the bidSource and deliveryDate) that should be shown
            by default in the UI
        value: This would normally be the objective value ('grand total') from the Shop result, or maybe the difference
            between the objective value in this run and for 'upper bound', but it should be possible to override it
            (e. g. if the difference is above some limit)
        alerts: An array of benchmarking calculation level Alerts.
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("bid_generated", "bid_source", "delivery_date", "is_selected", "name", "shop_result", "value", "workflow_execution_id",)
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (("alerts", dm.DirectRelationReference("power_ops_types", "calculationIssue")),)
    _direct_relations: ClassVar[tuple[str, ...]] = ("shop_result",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BenchmarkingResultDayAhead", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "BenchmarkingResultDayAhead")
    name: Optional[str] = None
    workflow_execution_id: Optional[str] = Field(None, alias="workflowExecutionId")
    bid_source: Union[str, dm.NodeId, None] = Field(default=None, alias="bidSource")
    delivery_date: Optional[datetime.date] = Field(None, alias="deliveryDate")
    bid_generated: Optional[datetime.datetime] = Field(None, alias="bidGenerated")
    shop_result: Union[ShopResultWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="shopResult")
    is_selected: Optional[bool] = Field(None, alias="isSelected")
    value: Optional[float] = None
    alerts: Optional[list[Union[AlertWrite, str, dm.NodeId]]] = Field(default=None, repr=False)

    @field_validator("shop_result", "alerts", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class BenchmarkingResultDayAheadList(DomainModelList[BenchmarkingResultDayAhead]):
    """List of benchmarking result day aheads in the read version."""

    _INSTANCE = BenchmarkingResultDayAhead
    def as_write(self) -> BenchmarkingResultDayAheadWriteList:
        """Convert these read versions of benchmarking result day ahead to the writing versions."""
        return BenchmarkingResultDayAheadWriteList([node.as_write() for node in self.data])


    @property
    def shop_result(self) -> ShopResultList:
        from ._shop_result import ShopResult, ShopResultList
        return ShopResultList([item.shop_result for item in self.data if isinstance(item.shop_result, ShopResult)])
    @property
    def alerts(self) -> AlertList:
        from ._alert import Alert, AlertList
        return AlertList([item for items in self.data for item in items.alerts or [] if isinstance(item, Alert)])


class BenchmarkingResultDayAheadWriteList(DomainModelWriteList[BenchmarkingResultDayAheadWrite]):
    """List of benchmarking result day aheads in the writing version."""

    _INSTANCE = BenchmarkingResultDayAheadWrite
    @property
    def shop_result(self) -> ShopResultWriteList:
        from ._shop_result import ShopResultWrite, ShopResultWriteList
        return ShopResultWriteList([item.shop_result for item in self.data if isinstance(item.shop_result, ShopResultWrite)])
    @property
    def alerts(self) -> AlertWriteList:
        from ._alert import AlertWrite, AlertWriteList
        return AlertWriteList([item for items in self.data for item in items.alerts or [] if isinstance(item, AlertWrite)])



def _create_benchmarking_result_day_ahead_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    workflow_execution_id: str | list[str] | None = None,
    workflow_execution_id_prefix: str | None = None,
    bid_source: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    min_delivery_date: datetime.date | None = None,
    max_delivery_date: datetime.date | None = None,
    min_bid_generated: datetime.datetime | None = None,
    max_bid_generated: datetime.datetime | None = None,
    shop_result: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    is_selected: bool | None = None,
    min_value: float | None = None,
    max_value: float | None = None,
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
    if isinstance(bid_source, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(bid_source):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bidSource"), value=as_instance_dict_id(bid_source)))
    if bid_source and isinstance(bid_source, Sequence) and not isinstance(bid_source, str) and not is_tuple_id(bid_source):
        filters.append(dm.filters.In(view_id.as_property_ref("bidSource"), values=[as_instance_dict_id(item) for item in bid_source]))
    if min_delivery_date is not None or max_delivery_date is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("deliveryDate"), gte=min_delivery_date.isoformat() if min_delivery_date else None, lte=max_delivery_date.isoformat() if max_delivery_date else None))
    if min_bid_generated is not None or max_bid_generated is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("bidGenerated"), gte=min_bid_generated.isoformat(timespec="milliseconds") if min_bid_generated else None, lte=max_bid_generated.isoformat(timespec="milliseconds") if max_bid_generated else None))
    if isinstance(shop_result, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(shop_result):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopResult"), value=as_instance_dict_id(shop_result)))
    if shop_result and isinstance(shop_result, Sequence) and not isinstance(shop_result, str) and not is_tuple_id(shop_result):
        filters.append(dm.filters.In(view_id.as_property_ref("shopResult"), values=[as_instance_dict_id(item) for item in shop_result]))
    if isinstance(is_selected, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isSelected"), value=is_selected))
    if min_value is not None or max_value is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("value"), gte=min_value, lte=max_value))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _BenchmarkingResultDayAheadQuery(NodeQueryCore[T_DomainModelList, BenchmarkingResultDayAheadList]):
    _view_id = BenchmarkingResultDayAhead._view_id
    _result_cls = BenchmarkingResultDayAhead
    _result_list_cls_end = BenchmarkingResultDayAheadList

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
        from ._shop_result import _ShopResultQuery

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


        if _ShopResultQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.shop_result = _ShopResultQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("shopResult"),
                    direction="outwards",
                ),
                connection_name="shop_result",
                connection_property=ViewPropertyId(self._view_id, "shopResult"),
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

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.workflow_execution_id = StringFilter(self, self._view_id.as_property_ref("workflowExecutionId"))
        self.bid_source_filter = DirectRelationFilter(self, self._view_id.as_property_ref("bidSource"))
        self.delivery_date = DateFilter(self, self._view_id.as_property_ref("deliveryDate"))
        self.bid_generated = TimestampFilter(self, self._view_id.as_property_ref("bidGenerated"))
        self.shop_result_filter = DirectRelationFilter(self, self._view_id.as_property_ref("shopResult"))
        self.is_selected = BooleanFilter(self, self._view_id.as_property_ref("isSelected"))
        self.value = FloatFilter(self, self._view_id.as_property_ref("value"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.workflow_execution_id,
            self.bid_source_filter,
            self.delivery_date,
            self.bid_generated,
            self.shop_result_filter,
            self.is_selected,
            self.value,
        ])

    def list_benchmarking_result_day_ahead(self, limit: int = DEFAULT_QUERY_LIMIT) -> BenchmarkingResultDayAheadList:
        return self._list(limit=limit)


class BenchmarkingResultDayAheadQuery(_BenchmarkingResultDayAheadQuery[BenchmarkingResultDayAheadList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, BenchmarkingResultDayAheadList)
