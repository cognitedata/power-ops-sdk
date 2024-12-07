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
    FloatFilter,
    TimestampFilter,
)
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._alert import Alert, AlertList, AlertGraphQL, AlertWrite, AlertWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_result import ShopResult, ShopResultList, ShopResultGraphQL, ShopResultWrite, ShopResultWriteList


__all__ = [
    "BenchmarkingResultDayAhead",
    "BenchmarkingResultDayAheadWrite",
    "BenchmarkingResultDayAheadApply",
    "BenchmarkingResultDayAheadList",
    "BenchmarkingResultDayAheadWriteList",
    "BenchmarkingResultDayAheadApplyList",
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
        is_selected: Indicating that this is the one result (for the bidSource and deliveryDate) that should be shown by default in the UI
        value: This would normally be the objective value ('grand total') from the Shop result, or maybe the difference between the objective value in this run and for 'upper bound', but it should be possible to override it (e. g. if the difference is above some limit)
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> BenchmarkingResultDayAhead:
        """Convert this GraphQL format of benchmarking result day ahead to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return BenchmarkingResultDayAhead(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            workflow_execution_id=self.workflow_execution_id,
            bid_source=self.bid_source,
            delivery_date=self.delivery_date,
            bid_generated=self.bid_generated,
            shop_result=self.shop_result.as_read()
if isinstance(self.shop_result, GraphQLCore)
else self.shop_result,
            is_selected=self.is_selected,
            value=self.value,
            alerts=[alert.as_read() for alert in self.alerts] if self.alerts is not None else None,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BenchmarkingResultDayAheadWrite:
        """Convert this GraphQL format of benchmarking result day ahead to the writing format."""
        return BenchmarkingResultDayAheadWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            workflow_execution_id=self.workflow_execution_id,
            bid_source=self.bid_source,
            delivery_date=self.delivery_date,
            bid_generated=self.bid_generated,
            shop_result=self.shop_result.as_write()
if isinstance(self.shop_result, GraphQLCore)
else self.shop_result,
            is_selected=self.is_selected,
            value=self.value,
            alerts=[alert.as_write() for alert in self.alerts] if self.alerts is not None else None,
        )


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
        is_selected: Indicating that this is the one result (for the bidSource and deliveryDate) that should be shown by default in the UI
        value: This would normally be the objective value ('grand total') from the Shop result, or maybe the difference between the objective value in this run and for 'upper bound', but it should be possible to override it (e. g. if the difference is above some limit)
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BenchmarkingResultDayAheadWrite:
        """Convert this read version of benchmarking result day ahead to the writing version."""
        return BenchmarkingResultDayAheadWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            workflow_execution_id=self.workflow_execution_id,
            bid_source=self.bid_source,
            delivery_date=self.delivery_date,
            bid_generated=self.bid_generated,
            shop_result=self.shop_result.as_write()
if isinstance(self.shop_result, DomainModel)
else self.shop_result,
            is_selected=self.is_selected,
            value=self.value,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts] if self.alerts is not None else None,
        )

    def as_apply(self) -> BenchmarkingResultDayAheadWrite:
        """Convert this read version of benchmarking result day ahead to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()
    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, BenchmarkingResultDayAhead],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._alert import Alert
        from ._shop_result import ShopResult
        for instance in instances.values():
            if isinstance(instance.shop_result, (dm.NodeId, str)) and (shop_result := nodes_by_id.get(instance.shop_result)) and isinstance(
                    shop_result, ShopResult
            ):
                instance.shop_result = shop_result
            if edges := edges_by_source_node.get(instance.as_id()):
                alerts: list[Alert | str | dm.NodeId] = []
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

                instance.alerts = alerts or None



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
        is_selected: Indicating that this is the one result (for the bidSource and deliveryDate) that should be shown by default in the UI
        value: This would normally be the objective value ('grand total') from the Shop result, or maybe the difference between the objective value in this run and for 'upper bound', but it should be possible to override it (e. g. if the difference is above some limit)
        alerts: An array of benchmarking calculation level Alerts.
    """

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

        if self.bid_source is not None:
            properties["bidSource"] = {
                "space":  self.space if isinstance(self.bid_source, str) else self.bid_source.space,
                "externalId": self.bid_source if isinstance(self.bid_source, str) else self.bid_source.external_id,
            }

        if self.delivery_date is not None or write_none:
            properties["deliveryDate"] = self.delivery_date.isoformat() if self.delivery_date else None

        if self.bid_generated is not None or write_none:
            properties["bidGenerated"] = self.bid_generated.isoformat(timespec="milliseconds") if self.bid_generated else None

        if self.shop_result is not None:
            properties["shopResult"] = {
                "space":  self.space if isinstance(self.shop_result, str) else self.shop_result.space,
                "externalId": self.shop_result if isinstance(self.shop_result, str) else self.shop_result.external_id,
            }

        if self.is_selected is not None or write_none:
            properties["isSelected"] = self.is_selected

        if self.value is not None or write_none:
            properties["value"] = self.value

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

        if isinstance(self.shop_result, DomainModelWrite):
            other_resources = self.shop_result._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class BenchmarkingResultDayAheadApply(BenchmarkingResultDayAheadWrite):
    def __new__(cls, *args, **kwargs) -> BenchmarkingResultDayAheadApply:
        warnings.warn(
            "BenchmarkingResultDayAheadApply is deprecated and will be removed in v1.0. Use BenchmarkingResultDayAheadWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "BenchmarkingResultDayAhead.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)

class BenchmarkingResultDayAheadList(DomainModelList[BenchmarkingResultDayAhead]):
    """List of benchmarking result day aheads in the read version."""

    _INSTANCE = BenchmarkingResultDayAhead
    def as_write(self) -> BenchmarkingResultDayAheadWriteList:
        """Convert these read versions of benchmarking result day ahead to the writing versions."""
        return BenchmarkingResultDayAheadWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> BenchmarkingResultDayAheadWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

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


class BenchmarkingResultDayAheadApplyList(BenchmarkingResultDayAheadWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
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
            connection_type,
            reverse_expression,
        )


        if _ShopResultQuery not in created_types:
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

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.workflow_execution_id = StringFilter(self, self._view_id.as_property_ref("workflowExecutionId"))
        self.delivery_date = DateFilter(self, self._view_id.as_property_ref("deliveryDate"))
        self.bid_generated = TimestampFilter(self, self._view_id.as_property_ref("bidGenerated"))
        self.is_selected = BooleanFilter(self, self._view_id.as_property_ref("isSelected"))
        self.value = FloatFilter(self, self._view_id.as_property_ref("value"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.workflow_execution_id,
            self.delivery_date,
            self.bid_generated,
            self.is_selected,
            self.value,
        ])

    def list_benchmarking_result_day_ahead(self, limit: int = DEFAULT_QUERY_LIMIT) -> BenchmarkingResultDayAheadList:
        return self._list(limit=limit)


class BenchmarkingResultDayAheadQuery(_BenchmarkingResultDayAheadQuery[BenchmarkingResultDayAheadList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, BenchmarkingResultDayAheadList)
