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

if TYPE_CHECKING:
    from ._alert import Alert, AlertGraphQL, AlertWrite
    from ._shop_result import ShopResult, ShopResultGraphQL, ShopResultWrite


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


BenchmarkingResultDayAheadTextFields = Literal["name", "workflow_execution_id"]
BenchmarkingResultDayAheadFields = Literal["name", "workflow_execution_id", "delivery_date", "bid_generated", "is_selected", "value"]

_BENCHMARKINGRESULTDAYAHEAD_PROPERTIES_BY_FIELD = {
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
    bid_source: Optional[str] = Field(default=None, alias="bidSource")
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
            space=self.space or DEFAULT_INSTANCE_SPACE,
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
            shop_result=self.shop_result.as_read() if isinstance(self.shop_result, GraphQLCore) else self.shop_result,
            is_selected=self.is_selected,
            value=self.value,
            alerts=[alert.as_read() for alert in self.alerts or []],
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BenchmarkingResultDayAheadWrite:
        """Convert this GraphQL format of benchmarking result day ahead to the writing format."""
        return BenchmarkingResultDayAheadWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            workflow_execution_id=self.workflow_execution_id,
            bid_source=self.bid_source,
            delivery_date=self.delivery_date,
            bid_generated=self.bid_generated,
            shop_result=self.shop_result.as_write() if isinstance(self.shop_result, GraphQLCore) else self.shop_result,
            is_selected=self.is_selected,
            value=self.value,
            alerts=[alert.as_write() for alert in self.alerts or []],
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
            shop_result=self.shop_result.as_write() if isinstance(self.shop_result, DomainModel) else self.shop_result,
            is_selected=self.is_selected,
            value=self.value,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
        )

    def as_apply(self) -> BenchmarkingResultDayAheadWrite:
        """Convert this read version of benchmarking result day ahead to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


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
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "BenchmarkingResultDayAhead")
    name: Optional[str] = None
    workflow_execution_id: Optional[str] = Field(None, alias="workflowExecutionId")
    bid_source: Union[str, dm.NodeId, None] = Field(default=None, alias="bidSource")
    delivery_date: Optional[datetime.date] = Field(None, alias="deliveryDate")
    bid_generated: Optional[datetime.datetime] = Field(None, alias="bidGenerated")
    shop_result: Union[ShopResultWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="shopResult")
    is_selected: Optional[bool] = Field(None, alias="isSelected")
    value: Optional[float] = None
    alerts: Optional[list[Union[AlertWrite, str, dm.NodeId]]] = Field(default=None, repr=False)

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


class BenchmarkingResultDayAheadWriteList(DomainModelWriteList[BenchmarkingResultDayAheadWrite]):
    """List of benchmarking result day aheads in the writing version."""

    _INSTANCE = BenchmarkingResultDayAheadWrite

class BenchmarkingResultDayAheadApplyList(BenchmarkingResultDayAheadWriteList): ...



def _create_benchmarking_result_day_ahead_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    workflow_execution_id: str | list[str] | None = None,
    workflow_execution_id_prefix: str | None = None,
    bid_source: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    min_delivery_date: datetime.date | None = None,
    max_delivery_date: datetime.date | None = None,
    min_bid_generated: datetime.datetime | None = None,
    max_bid_generated: datetime.datetime | None = None,
    shop_result: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if bid_source and isinstance(bid_source, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bidSource"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": bid_source}))
    if bid_source and isinstance(bid_source, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bidSource"), value={"space": bid_source[0], "externalId": bid_source[1]}))
    if bid_source and isinstance(bid_source, list) and isinstance(bid_source[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("bidSource"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in bid_source]))
    if bid_source and isinstance(bid_source, list) and isinstance(bid_source[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("bidSource"), values=[{"space": item[0], "externalId": item[1]} for item in bid_source]))
    if min_delivery_date is not None or max_delivery_date is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("deliveryDate"), gte=min_delivery_date.isoformat() if min_delivery_date else None, lte=max_delivery_date.isoformat() if max_delivery_date else None))
    if min_bid_generated is not None or max_bid_generated is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("bidGenerated"), gte=min_bid_generated.isoformat(timespec="milliseconds") if min_bid_generated else None, lte=max_bid_generated.isoformat(timespec="milliseconds") if max_bid_generated else None))
    if shop_result and isinstance(shop_result, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopResult"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": shop_result}))
    if shop_result and isinstance(shop_result, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopResult"), value={"space": shop_result[0], "externalId": shop_result[1]}))
    if shop_result and isinstance(shop_result, list) and isinstance(shop_result[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("shopResult"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in shop_result]))
    if shop_result and isinstance(shop_result, list) and isinstance(shop_result[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("shopResult"), values=[{"space": item[0], "externalId": item[1]} for item in shop_result]))
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
