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
    DateFilter,
    TimestampFilter,
)
from cognite.powerops.client._generated.v1.data_classes._shop_case import ShopCase, ShopCaseWrite
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._shop_file import ShopFile, ShopFileList, ShopFileGraphQL, ShopFileWrite, ShopFileWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_scenario import ShopScenario, ShopScenarioList, ShopScenarioGraphQL, ShopScenarioWrite, ShopScenarioWriteList


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


BenchmarkingShopCaseTextFields = Literal["external_id", ]
BenchmarkingShopCaseFields = Literal["external_id", "start_time", "end_time", "delivery_date", "bid_generated"]

_BENCHMARKINGSHOPCASE_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
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
    bid_source: Optional[dict] = Field(default=None, alias="bidSource")
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
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            scenario=self.scenario.as_read()
if isinstance(self.scenario, GraphQLCore)
else self.scenario,
            start_time=self.start_time,
            end_time=self.end_time,
            shop_files=[shop_file.as_read() for shop_file in self.shop_files] if self.shop_files is not None else None,
            bid_source=self.bid_source,
            delivery_date=self.delivery_date,
            bid_generated=self.bid_generated,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BenchmarkingShopCaseWrite:
        """Convert this GraphQL format of benchmarking shop case to the writing format."""
        return BenchmarkingShopCaseWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            scenario=self.scenario.as_write()
if isinstance(self.scenario, GraphQLCore)
else self.scenario,
            start_time=self.start_time,
            end_time=self.end_time,
            shop_files=[shop_file.as_write() for shop_file in self.shop_files] if self.shop_files is not None else None,
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BenchmarkingShopCaseWrite:
        """Convert this read version of benchmarking shop case to the writing version."""
        return BenchmarkingShopCaseWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            scenario=self.scenario.as_write()
if isinstance(self.scenario, DomainModel)
else self.scenario,
            start_time=self.start_time,
            end_time=self.end_time,
            shop_files=[shop_file.as_write() if isinstance(shop_file, DomainModel) else shop_file for shop_file in self.shop_files] if self.shop_files is not None else None,
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
    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, BenchmarkingShopCase],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._shop_file import ShopFile
        from ._shop_scenario import ShopScenario
        for instance in instances.values():
            if isinstance(instance.scenario, (dm.NodeId, str)) and (scenario := nodes_by_id.get(instance.scenario)) and isinstance(
                    scenario, ShopScenario
            ):
                instance.scenario = scenario
            if edges := edges_by_source_node.get(instance.as_id()):
                shop_files: list[ShopFile | str | dm.NodeId] = []
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

                    if edge_type == dm.DirectRelationReference("power_ops_types", "ShopCase.shopFiles") and isinstance(
                        value, (ShopFile, str, dm.NodeId)
                    ):
                        shop_files.append(value)

                instance.shop_files = shop_files or None



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

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "BenchmarkingShopCase")
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
                type=as_direct_relation_reference(self.node_type),
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

    @property
    def scenario(self) -> ShopScenarioList:
        from ._shop_scenario import ShopScenario, ShopScenarioList
        return ShopScenarioList([item.scenario for item in self.data if isinstance(item.scenario, ShopScenario)])
    @property
    def shop_files(self) -> ShopFileList:
        from ._shop_file import ShopFile, ShopFileList
        return ShopFileList([item for items in self.data for item in items.shop_files or [] if isinstance(item, ShopFile)])


class BenchmarkingShopCaseWriteList(DomainModelWriteList[BenchmarkingShopCaseWrite]):
    """List of benchmarking shop cases in the writing version."""

    _INSTANCE = BenchmarkingShopCaseWrite
    @property
    def scenario(self) -> ShopScenarioWriteList:
        from ._shop_scenario import ShopScenarioWrite, ShopScenarioWriteList
        return ShopScenarioWriteList([item.scenario for item in self.data if isinstance(item.scenario, ShopScenarioWrite)])
    @property
    def shop_files(self) -> ShopFileWriteList:
        from ._shop_file import ShopFileWrite, ShopFileWriteList
        return ShopFileWriteList([item for items in self.data for item in items.shop_files or [] if isinstance(item, ShopFileWrite)])


class BenchmarkingShopCaseApplyList(BenchmarkingShopCaseWriteList): ...


def _create_benchmarking_shop_case_filter(
    view_id: dm.ViewId,
    scenario: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    min_start_time: datetime.datetime | None = None,
    max_start_time: datetime.datetime | None = None,
    min_end_time: datetime.datetime | None = None,
    max_end_time: datetime.datetime | None = None,
    bid_source: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    min_delivery_date: datetime.date | None = None,
    max_delivery_date: datetime.date | None = None,
    min_bid_generated: datetime.datetime | None = None,
    max_bid_generated: datetime.datetime | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(scenario, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(scenario):
        filters.append(dm.filters.Equals(view_id.as_property_ref("scenario"), value=as_instance_dict_id(scenario)))
    if scenario and isinstance(scenario, Sequence) and not isinstance(scenario, str) and not is_tuple_id(scenario):
        filters.append(dm.filters.In(view_id.as_property_ref("scenario"), values=[as_instance_dict_id(item) for item in scenario]))
    if min_start_time is not None or max_start_time is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("startTime"), gte=min_start_time.isoformat(timespec="milliseconds") if min_start_time else None, lte=max_start_time.isoformat(timespec="milliseconds") if max_start_time else None))
    if min_end_time is not None or max_end_time is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("endTime"), gte=min_end_time.isoformat(timespec="milliseconds") if min_end_time else None, lte=max_end_time.isoformat(timespec="milliseconds") if max_end_time else None))
    if isinstance(bid_source, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(bid_source):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bidSource"), value=as_instance_dict_id(bid_source)))
    if bid_source and isinstance(bid_source, Sequence) and not isinstance(bid_source, str) and not is_tuple_id(bid_source):
        filters.append(dm.filters.In(view_id.as_property_ref("bidSource"), values=[as_instance_dict_id(item) for item in bid_source]))
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


class _BenchmarkingShopCaseQuery(NodeQueryCore[T_DomainModelList, BenchmarkingShopCaseList]):
    _view_id = BenchmarkingShopCase._view_id
    _result_cls = BenchmarkingShopCase
    _result_list_cls_end = BenchmarkingShopCaseList

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
        from ._shop_file import _ShopFileQuery
        from ._shop_scenario import _ShopScenarioQuery

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

        if _ShopScenarioQuery not in created_types:
            self.scenario = _ShopScenarioQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("scenario"),
                    direction="outwards",
                ),
                connection_name="scenario",
            )

        if _ShopFileQuery not in created_types:
            self.shop_files = _ShopFileQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="shop_files",
            )


        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.start_time = TimestampFilter(self, self._view_id.as_property_ref("startTime"))
        self.end_time = TimestampFilter(self, self._view_id.as_property_ref("endTime"))
        self.delivery_date = DateFilter(self, self._view_id.as_property_ref("deliveryDate"))
        self.bid_generated = TimestampFilter(self, self._view_id.as_property_ref("bidGenerated"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.start_time,
            self.end_time,
            self.delivery_date,
            self.bid_generated,
        ])

    def list_benchmarking_shop_case(self, limit: int = DEFAULT_QUERY_LIMIT) -> BenchmarkingShopCaseList:
        return self._list(limit=limit)


class BenchmarkingShopCaseQuery(_BenchmarkingShopCaseQuery[BenchmarkingShopCaseList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, BenchmarkingShopCaseList)
