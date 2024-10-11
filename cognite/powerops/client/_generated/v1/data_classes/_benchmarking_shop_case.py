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
from ._shop_case import ShopCase, ShopCaseWrite

if TYPE_CHECKING:
    from ._shop_file import ShopFile, ShopFileGraphQL, ShopFileWrite
    from ._shop_scenario import ShopScenario, ShopScenarioGraphQL, ShopScenarioWrite


__all__ = [
    "BenchmarkingShopCase",
    "BenchmarkingShopCaseWrite",
    "BenchmarkingShopCaseApply",
    "BenchmarkingShopCaseList",
    "BenchmarkingShopCaseWriteList",
    "BenchmarkingShopCaseApplyList",
    "BenchmarkingShopCaseFields",

    "BenchmarkingShopCaseGraphQL",
]

BenchmarkingShopCaseFields = Literal["start_time", "end_time", "delivery_date", "bid_generated"]

_BENCHMARKINGSHOPCASE_PROPERTIES_BY_FIELD = {
    "start_time": "startTime",
    "end_time": "endTime",
    "delivery_date": "deliveryDate",
    "bid_generated": "bidGenerated",
}

class BenchmarkingShopCaseGraphQL(GraphQLCore):
    """This represents the reading version of benchmarking shop case, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the benchmarking shop case.
        data_record: The data record of the benchmarking shop case node.
        scenario: The Shop scenario that was used to produce this result
        start_time: The start time of the case
        end_time: The end time of the case
        shop_files: The list of shop files that are used in a shop run. This encompasses all shop files such as case, module series, cut files etc.
        bid_source: The bid source field.
        delivery_date: The delivery date
        bid_generated: Timestamp of when the bid had been generated
    """
    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BenchmarkingShopCase", "1")
    scenario: Optional[ShopScenarioGraphQL] = Field(default=None, repr=False)
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")
    shop_files: Optional[list[ShopFileGraphQL]] = Field(default=None, repr=False, alias="shopFiles")
    bid_source: Optional[str] = Field(default=None, alias="bidSource")
    delivery_date: Optional[datetime.date] = Field(None, alias="deliveryDate")
    bid_generated: Optional[datetime.datetime] = Field(None, alias="bidGenerated")

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
    @field_validator("scenario", "shop_files", "bid_source", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> BenchmarkingShopCase:
        """Convert this GraphQL format of benchmarking shop case to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return BenchmarkingShopCase(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            scenario=self.scenario.as_read() if isinstance(self.scenario, GraphQLCore) else self.scenario,
            start_time=self.start_time,
            end_time=self.end_time,
            shop_files=[shop_file.as_read() for shop_file in self.shop_files or []],
            bid_source=self.bid_source,
            delivery_date=self.delivery_date,
            bid_generated=self.bid_generated,
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BenchmarkingShopCaseWrite:
        """Convert this GraphQL format of benchmarking shop case to the writing format."""
        return BenchmarkingShopCaseWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            scenario=self.scenario.as_write() if isinstance(self.scenario, GraphQLCore) else self.scenario,
            start_time=self.start_time,
            end_time=self.end_time,
            shop_files=[shop_file.as_write() for shop_file in self.shop_files or []],
            bid_source=self.bid_source,
            delivery_date=self.delivery_date,
            bid_generated=self.bid_generated,
        )


class BenchmarkingShopCase(ShopCase):
    """This represents the reading version of benchmarking shop case.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the benchmarking shop case.
        data_record: The data record of the benchmarking shop case node.
        scenario: The Shop scenario that was used to produce this result
        start_time: The start time of the case
        end_time: The end time of the case
        shop_files: The list of shop files that are used in a shop run. This encompasses all shop files such as case, module series, cut files etc.
        bid_source: The bid source field.
        delivery_date: The delivery date
        bid_generated: Timestamp of when the bid had been generated
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BenchmarkingShopCase", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "BenchmarkingShopCase")
    bid_source: Union[str, dm.NodeId, None] = Field(default=None, alias="bidSource")
    delivery_date: Optional[datetime.date] = Field(None, alias="deliveryDate")
    bid_generated: Optional[datetime.datetime] = Field(None, alias="bidGenerated")

    def as_write(self) -> BenchmarkingShopCaseWrite:
        """Convert this read version of benchmarking shop case to the writing version."""
        return BenchmarkingShopCaseWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            scenario=self.scenario.as_write() if isinstance(self.scenario, DomainModel) else self.scenario,
            start_time=self.start_time,
            end_time=self.end_time,
            shop_files=[shop_file.as_write() if isinstance(shop_file, DomainModel) else shop_file for shop_file in self.shop_files or []],
            bid_source=self.bid_source,
            delivery_date=self.delivery_date,
            bid_generated=self.bid_generated,
        )

    def as_apply(self) -> BenchmarkingShopCaseWrite:
        """Convert this read version of benchmarking shop case to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BenchmarkingShopCaseWrite(ShopCaseWrite):
    """This represents the writing version of benchmarking shop case.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the benchmarking shop case.
        data_record: The data record of the benchmarking shop case node.
        scenario: The Shop scenario that was used to produce this result
        start_time: The start time of the case
        end_time: The end time of the case
        shop_files: The list of shop files that are used in a shop run. This encompasses all shop files such as case, module series, cut files etc.
        bid_source: The bid source field.
        delivery_date: The delivery date
        bid_generated: Timestamp of when the bid had been generated
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BenchmarkingShopCase", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "BenchmarkingShopCase")
    bid_source: Union[str, dm.NodeId, None] = Field(default=None, alias="bidSource")
    delivery_date: Optional[datetime.date] = Field(None, alias="deliveryDate")
    bid_generated: Optional[datetime.datetime] = Field(None, alias="bidGenerated")

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

        if self.scenario is not None:
            properties["scenario"] = {
                "space":  self.space if isinstance(self.scenario, str) else self.scenario.space,
                "externalId": self.scenario if isinstance(self.scenario, str) else self.scenario.external_id,
            }

        if self.start_time is not None or write_none:
            properties["startTime"] = self.start_time.isoformat(timespec="milliseconds") if self.start_time else None

        if self.end_time is not None or write_none:
            properties["endTime"] = self.end_time.isoformat(timespec="milliseconds") if self.end_time else None

        if self.bid_source is not None:
            properties["bidSource"] = {
                "space":  self.space if isinstance(self.bid_source, str) else self.bid_source.space,
                "externalId": self.bid_source if isinstance(self.bid_source, str) else self.bid_source.external_id,
            }

        if self.delivery_date is not None or write_none:
            properties["deliveryDate"] = self.delivery_date.isoformat() if self.delivery_date else None

        if self.bid_generated is not None or write_none:
            properties["bidGenerated"] = self.bid_generated.isoformat(timespec="milliseconds") if self.bid_generated else None


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



        edge_type = dm.DirectRelationReference("power_ops_types", "ShopCase.shopFiles")
        for shop_file in self.shop_files or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=shop_file,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.scenario, DomainModelWrite):
            other_resources = self.scenario._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class BenchmarkingShopCaseApply(BenchmarkingShopCaseWrite):
    def __new__(cls, *args, **kwargs) -> BenchmarkingShopCaseApply:
        warnings.warn(
            "BenchmarkingShopCaseApply is deprecated and will be removed in v1.0. Use BenchmarkingShopCaseWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "BenchmarkingShopCase.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class BenchmarkingShopCaseList(DomainModelList[BenchmarkingShopCase]):
    """List of benchmarking shop cases in the read version."""

    _INSTANCE = BenchmarkingShopCase

    def as_write(self) -> BenchmarkingShopCaseWriteList:
        """Convert these read versions of benchmarking shop case to the writing versions."""
        return BenchmarkingShopCaseWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> BenchmarkingShopCaseWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BenchmarkingShopCaseWriteList(DomainModelWriteList[BenchmarkingShopCaseWrite]):
    """List of benchmarking shop cases in the writing version."""

    _INSTANCE = BenchmarkingShopCaseWrite

class BenchmarkingShopCaseApplyList(BenchmarkingShopCaseWriteList): ...



def _create_benchmarking_shop_case_filter(
    view_id: dm.ViewId,
    scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    min_start_time: datetime.datetime | None = None,
    max_start_time: datetime.datetime | None = None,
    min_end_time: datetime.datetime | None = None,
    max_end_time: datetime.datetime | None = None,
    bid_source: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    min_delivery_date: datetime.date | None = None,
    max_delivery_date: datetime.date | None = None,
    min_bid_generated: datetime.datetime | None = None,
    max_bid_generated: datetime.datetime | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if scenario and isinstance(scenario, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("scenario"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": scenario}))
    if scenario and isinstance(scenario, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("scenario"), value={"space": scenario[0], "externalId": scenario[1]}))
    if scenario and isinstance(scenario, list) and isinstance(scenario[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("scenario"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in scenario]))
    if scenario and isinstance(scenario, list) and isinstance(scenario[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("scenario"), values=[{"space": item[0], "externalId": item[1]} for item in scenario]))
    if min_start_time is not None or max_start_time is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("startTime"), gte=min_start_time.isoformat(timespec="milliseconds") if min_start_time else None, lte=max_start_time.isoformat(timespec="milliseconds") if max_start_time else None))
    if min_end_time is not None or max_end_time is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("endTime"), gte=min_end_time.isoformat(timespec="milliseconds") if min_end_time else None, lte=max_end_time.isoformat(timespec="milliseconds") if max_end_time else None))
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
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
