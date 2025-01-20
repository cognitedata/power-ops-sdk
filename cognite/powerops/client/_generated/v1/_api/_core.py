from __future__ import annotations

from abc import ABC
from collections import defaultdict
from collections.abc import Sequence
from itertools import groupby
from typing import (
    Generic,
    Literal,
    Any,
    Iterator,
    Protocol,
    SupportsIndex,
    TypeVar,
    overload,
    ClassVar,
)

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeriesList
from cognite.client.data_classes.data_modeling.instances import InstanceSort, InstanceAggregationResultList
from pydantic import BaseModel, TypeAdapter, ValidationError

from cognite.powerops.client._generated.v1.config import global_config
from cognite.powerops.client._generated.v1 import data_classes
from cognite.powerops.client._generated.v1.data_classes._core import (
    chunker,
    DomainModel,
    DomainModelWrite,
    DEFAULT_INSTANCE_SPACE,
    IN_FILTER_CHUNK_SIZE,
    PageInfo,
    GraphQLCore,
    GraphQLList,
    ResourcesWriteResult,
    T_DomainModel,
    T_DomainModelWrite,
    T_DomainModelWriteList,
    T_DomainModelList,
    T_DomainRelation,
    T_DomainRelationWrite,
    T_DomainRelationList,
    QueryBuilder,
    QueryUnpacker,
)

DEFAULT_LIMIT_READ = 25
DEFAULT_QUERY_LIMIT = 3
IN_FILTER_LIMIT = 5_000
INSTANCE_QUERY_LIMIT = 1_000
NODE_PROPERTIES = {"externalId", "space"}

Aggregations = Literal["avg", "count", "max", "min", "sum"]

_METRIC_AGGREGATIONS_BY_NAME = {
    "avg": dm.aggregations.Avg,
    "count": dm.aggregations.Count,
    "max": dm.aggregations.Max,
    "min": dm.aggregations.Min,
    "sum": dm.aggregations.Sum,
}

_T_co = TypeVar("_T_co", covariant=True)


def _as_node_id(value: str | dm.NodeId | tuple[str, str], space: str) -> dm.NodeId:
    if isinstance(value, str):
        return dm.NodeId(space=space, external_id=value)
    if isinstance(value, dm.NodeId):
        return value
    if isinstance(value, tuple):
        return dm.NodeId(space=value[0], external_id=value[1])
    raise TypeError(f"Expected str, NodeId or tuple, got {type(value)}")


# Source from https://github.com/python/typing/issues/256#issuecomment-1442633430
# This works because str.__contains__ does not accept an object (either in typeshed or at runtime)
class SequenceNotStr(Protocol[_T_co]):
    @overload
    def __getitem__(self, index: SupportsIndex, /) -> _T_co: ...

    @overload
    def __getitem__(self, index: slice, /) -> Sequence[_T_co]: ...

    def __contains__(self, value: object, /) -> bool: ...

    def __len__(self) -> int: ...

    def __iter__(self) -> Iterator[_T_co]: ...

    def index(self, value: Any, /, start: int = 0, stop: int = ...) -> int: ...

    def count(self, value: Any, /) -> int: ...

    def __reversed__(self) -> Iterator[_T_co]: ...


class NodeReadAPI(Generic[T_DomainModel, T_DomainModelList], ABC):
    _view_id: ClassVar[dm.ViewId]
    _properties_by_field: ClassVar[dict[str, str]]
    _direct_children_by_external_id: ClassVar[dict[str, type[DomainModel]]]
    _class_type: type[T_DomainModel]
    _class_list: type[T_DomainModelList]

    def __init__(self, client: CogniteClient):
        self._client = client

    def _delete(self, external_id: str | SequenceNotStr[str], space: str) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    def _retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
        as_child_class: SequenceNotStr[str] | None = None,
    ) -> T_DomainModel | T_DomainModelList | None:
        if isinstance(external_id, str | dm.NodeId) or (
            isinstance(external_id, tuple) and len(external_id) == 2 and all(isinstance(i, str) for i in external_id)
        ):
            node_ids = [_as_node_id(external_id, space)]
            is_multiple = False
        else:
            is_multiple = True
            node_ids = [_as_node_id(ext_id, space) for ext_id in external_id]

        items: list[DomainModel] = []
        if as_child_class and retrieve_connections == "skip":
            if not hasattr(self, "_direct_children_by_external_id"):
                raise ValueError(f"{type(self).__name__} does not have any direct children")
            for child_class_external_id in as_child_class:
                child_cls = self._direct_children_by_external_id.get(child_class_external_id)
                if child_cls is None:
                    raise ValueError(f"Could not find child class with external_id {child_class_external_id}")
                instances = self._client.data_modeling.instances.retrieve(nodes=node_ids, sources=child_cls._view_id)
                items.extend(
                    instantiate_classes(child_cls, [child_cls._to_dict(node) for node in instances.nodes], "retrieve")
                )
        elif as_child_class:
            raise ValueError("Cannot retrieve as child classes and include connections")
        elif retrieve_connections == "skip":
            instances = self._client.data_modeling.instances.retrieve(nodes=node_ids, sources=self._view_id)
            items.extend(
                instantiate_classes(
                    self._class_type, [self._class_type._to_dict(node) for node in instances.nodes], "retrieve"
                )
            )
        else:
            for space_key, external_ids in groupby(
                sorted((node_id.as_tuple() for node_id in node_ids)), key=lambda x: x[0]
            ):
                external_id_list = [ext_id[1] for ext_id in external_ids]
                for ext_id_chunk in chunker(external_id_list, IN_FILTER_CHUNK_SIZE):
                    filter_ = dm.filters.Equals(["node", "space"], space_key) & dm.filters.In(
                        ["node", "externalId"], ext_id_chunk
                    )
                    items.extend(
                        instantiate_classes(
                            self._class_type, self._query(filter_, len(ext_id_chunk), retrieve_connections), "retrieve"
                        )
                    )

        nodes = self._class_list(items)

        if is_multiple:
            return nodes
        elif not nodes:
            return None
        else:
            return nodes[0]

    def _query(
        self,
        filter_: dm.Filter | None,
        limit: int,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def _search(
        self,
        query: str,
        properties: str | SequenceNotStr[str] | None = None,
        filter_: dm.Filter | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        sort_by: str | list[str] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> T_DomainModelList:
        properties_input = self._to_input_properties(properties)

        sort_input = self._create_sort(sort_by, direction, sort)
        nodes = self._client.data_modeling.instances.search(
            view=self._view_id,
            query=query,
            instance_type="node",
            properties=properties_input,
            filter=filter_,
            limit=limit,
            sort=sort_input,
        )
        return self._class_list(
            instantiate_classes(self._class_type, [self._class_type._to_dict(node) for node in nodes], "search")
        )

    def _to_input_properties(self, properties: str | SequenceNotStr[str] | None) -> list[str] | None:
        properties_input: list[str] | None = None
        if isinstance(properties, str):
            properties_input = [properties]
        elif isinstance(properties, Sequence):
            properties_input = list(properties)
        if properties_input:
            properties_input = [self._properties_by_field.get(prop, prop) for prop in properties_input]
        return properties_input

    def _aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation]
        ),
        group_by: str | SequenceNotStr[str] | None = None,
        properties: str | SequenceNotStr[str] | None = None,
        query: str | None = None,
        search_properties: str | SequenceNotStr[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        if isinstance(group_by, str):
            group_by = [group_by]

        if group_by:
            group_by = [self._properties_by_field.get(prop, prop) for prop in group_by]

        search_properties_input = self._to_input_properties(search_properties)

        if isinstance(properties, str):
            properties = [properties]

        if properties:
            properties = [self._properties_by_field.get(prop, prop) for prop in properties]

        is_single_aggregate = False
        if isinstance(aggregate, str | dm.aggregations.MetricAggregation):
            aggregate = [aggregate]
            is_single_aggregate = True

        if properties is None and (invalid := [agg for agg in aggregate if isinstance(agg, str) and agg != "count"]):
            raise ValueError(f"Cannot aggregate on {invalid} without specifying properties")

        aggregates: list[dm.aggregations.MetricAggregation] = []
        for agg in aggregate:
            if isinstance(agg, dm.aggregations.MetricAggregation):
                aggregates.append(agg)
            elif isinstance(agg, str):
                if agg == "count" and properties is None:
                    aggregates.append(dm.aggregations.Count("externalId"))
                elif properties is None:
                    raise ValueError(f"Cannot aggregate on {agg} without specifying properties")
                else:
                    for prop in properties:
                        aggregates.append(_METRIC_AGGREGATIONS_BY_NAME[agg](prop))
            else:
                raise TypeError(f"Expected str or MetricAggregation, got {type(agg)}")

        return self._client.data_modeling.instances.aggregate(
            view=self._view_id,
            aggregates=aggregates[0] if is_single_aggregate else aggregates,
            group_by=group_by,
            instance_type="node",
            query=query,
            properties=search_properties_input,
            filter=filter,
            limit=limit,
        )

    def _histogram(
        self,
        property: str,
        interval: float,
        query: str | None = None,
        search_properties: str | SequenceNotStr[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        property = self._properties_by_field.get(property, property)

        if isinstance(search_properties, str):
            search_properties = [search_properties]
        if search_properties:
            search_properties = [self._properties_by_field.get(prop, prop) for prop in search_properties]

        return self._client.data_modeling.instances.histogram(
            view=self._view_id,
            histograms=dm.aggregations.Histogram(property, interval),
            instance_type="node",
            query=query,
            properties=search_properties,
            filter=filter,
            limit=limit,
        )

    def _list(self, limit: int, filter: dm.Filter | None, sort: list[InstanceSort] | None = None) -> T_DomainModelList:
        nodes = self._client.data_modeling.instances.list(
            instance_type="node",
            sources=self._view_id,
            limit=limit,
            filter=filter,
            sort=sort,
        )
        return self._class_list(
            instantiate_classes(self._class_type, [self._class_type._to_dict(node) for node in nodes], "list")
        )

    def _create_sort(
        self,
        sort_by: str | list[str] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> list[InstanceSort] | None:
        sort_input: list[InstanceSort] | None = None
        if sort is None and isinstance(sort_by, str):
            sort_input = [self._create_sort_entry(sort_by, direction)]
        elif sort is None and isinstance(sort_by, list):
            sort_input = [self._create_sort_entry(sort_by_, direction) for sort_by_ in sort_by]
        elif sort is not None:
            sort_input = sort if isinstance(sort, list) else [sort]
            for sort_ in sort_input:
                if isinstance(sort_.property, Sequence) and len(sort_.property) == 1:
                    sort_.property = self._create_property_reference(sort_.property[0])
                elif isinstance(sort_.property, str):
                    sort_.property = self._create_property_reference(sort_.property)
        return sort_input

    def _create_sort_entry(self, sort_by: str, direction: Literal["ascending", "descending"]) -> InstanceSort:
        return InstanceSort(self._create_property_reference(sort_by), direction)

    def _create_property_reference(self, property_: str) -> list[str] | tuple[str, ...]:
        prop_name = self._properties_by_field.get(property_, property_)
        if prop_name in NODE_PROPERTIES:
            return ["node", prop_name]
        else:
            return self._view_id.as_property_ref(prop_name)


class NodeAPI(
    Generic[T_DomainModel, T_DomainModelWrite, T_DomainModelList, T_DomainModelWriteList],
    NodeReadAPI[T_DomainModel, T_DomainModelList],
    ABC,
):
    _class_write_list: type[T_DomainModelWriteList]

    def _apply(
        self, item: T_DomainModelWrite | Sequence[T_DomainModelWrite], replace: bool = False, write_none: bool = False
    ) -> ResourcesWriteResult:
        if isinstance(item, DomainModelWrite):
            instances = item.to_instances_write(write_none)
        else:
            instances = self._class_write_list(item).to_instances_write(write_none)
        result = self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )
        time_series = TimeSeriesList([])
        if instances.time_series:
            time_series = self._client.time_series.upsert(instances.time_series, mode="patch")

        return ResourcesWriteResult(result.nodes, result.edges, time_series)


class EdgeAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def _list(
        self,
        limit: int = DEFAULT_LIMIT_READ,
        filter_: dm.Filter | None = None,
    ) -> dm.EdgeList:
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=filter_)


class EdgePropertyAPI(EdgeAPI, Generic[T_DomainRelation, T_DomainRelationWrite, T_DomainRelationList], ABC):
    _view_id: ClassVar[dm.ViewId]
    _class_type: type[T_DomainRelation]
    _class_write_type: type[T_DomainRelationWrite]
    _class_list: type[T_DomainRelationList]

    def _list(  # type: ignore[override]
        self,
        limit: int = DEFAULT_LIMIT_READ,
        filter_: dm.Filter | None = None,
    ) -> T_DomainRelationList:
        edges = self._client.data_modeling.instances.list("edge", limit=limit, filter=filter_, sources=[self._view_id])
        return self._class_list(
            instantiate_classes(self._class_type, [self._class_type._to_dict(edge) for edge in edges], "list")
        )


class QueryAPI(Generic[T_DomainModel, T_DomainModelList]):
    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder,
        result_cls: type[T_DomainModel],
        result_list_cls: type[T_DomainModelList],
    ):
        self._client = client
        self._builder = builder
        self._result_cls = result_cls
        self._result_list_cls = result_list_cls

    def _query(self) -> T_DomainModelList:
        self._builder.execute_query(self._client, remove_not_connected=True)
        unpacked = QueryUnpacker(self._builder).unpack()
        item_list = instantiate_classes(self._result_cls, unpacked, "query")
        return self._result_list_cls(item_list)


def _create_edge_filter(
    edge_type: dm.DirectRelationReference,
    start_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    start_node_space: str = DEFAULT_INSTANCE_SPACE,
    end_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    space_end_node: str = DEFAULT_INSTANCE_SPACE,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter:
    filters: list[dm.Filter] = [
        dm.filters.Equals(
            ["edge", "type"],
            {"space": edge_type.space, "externalId": edge_type.external_id},
        )
    ]
    if start_node and isinstance(start_node, str):
        filters.append(
            dm.filters.Equals(["edge", "startNode"], value={"space": start_node_space, "externalId": start_node})
        )
    if start_node and isinstance(start_node, dm.NodeId):
        filters.append(
            dm.filters.Equals(
                ["edge", "startNode"], value=start_node.dump(camel_case=True, include_instance_type=False)
            )
        )
    if start_node and isinstance(start_node, list):
        filters.append(
            dm.filters.In(
                ["edge", "startNode"],
                values=[
                    (
                        {"space": start_node_space, "externalId": ext_id}
                        if isinstance(ext_id, str)
                        else ext_id.dump(camel_case=True, include_instance_type=False)
                    )
                    for ext_id in start_node
                ],
            )
        )
    if end_node and isinstance(end_node, str):
        filters.append(dm.filters.Equals(["edge", "endNode"], value={"space": space_end_node, "externalId": end_node}))
    if end_node and isinstance(end_node, dm.NodeId):
        filters.append(
            dm.filters.Equals(["edge", "endNode"], value=end_node.dump(camel_case=True, include_instance_type=False))
        )
    if end_node and isinstance(end_node, list):
        filters.append(
            dm.filters.In(
                ["edge", "endNode"],
                values=[
                    (
                        {"space": space_end_node, "externalId": ext_id}
                        if isinstance(ext_id, str)
                        else ext_id.dump(camel_case=True, include_instance_type=False)
                    )
                    for ext_id in end_node
                ],
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["edge", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["edge", "space"], value=space))
    if space and (space, list):
        filters.append(dm.filters.In(["edge", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters)


class GraphQLQueryResponse:
    def __init__(self, data_model_id: dm.DataModelId):
        self._output = GraphQLList([])
        self._data_class_by_type = _GRAPHQL_DATA_CLASS_BY_DATA_MODEL_BY_TYPE[data_model_id]

    def parse(self, response: dict[str, Any]) -> GraphQLList:
        if "errors" in response:
            raise RuntimeError(response["errors"])
        _, data = next(iter(response.items()))
        self._parse_item(data)
        if "pageInfo" in data:
            self._output.page_info = PageInfo.load(data["pageInfo"])
        return self._output

    def _parse_item(self, data: dict[str, Any]) -> None:
        if "items" in data:
            for item in data["items"]:
                self._parse_item(item)
        elif "__typename" in data:
            try:
                item = self._data_class_by_type[data["__typename"]].model_validate(data)
            except KeyError:
                raise ValueError(f"Could not find class for type {data['__typename']}") from None
            else:
                self._output.append(item)
        else:
            raise RuntimeError("Missing '__typename' in GraphQL response. Cannot determine the type of the response.")


_GRAPHQL_DATA_CLASS_BY_DATA_MODEL_BY_TYPE: dict[dm.DataModelId, dict[str, type[GraphQLCore]]] = {
    dm.DataModelId("power_ops_core", "compute_ShopBasedDayAhead", "1"): {
        "TaskDispatcherInput": data_classes.TaskDispatcherInputGraphQL,
        "TaskDispatcherOutput": data_classes.TaskDispatcherOutputGraphQL,
        "ShopPreprocessorInput": data_classes.ShopPreprocessorInputGraphQL,
        "ShopPreprocessorOutput": data_classes.ShopPreprocessorOutputGraphQL,
        "ShopTriggerInput": data_classes.ShopTriggerInputGraphQL,
        "ShopTriggerOutput": data_classes.ShopTriggerOutputGraphQL,
        "PartialBidMatrixCalculationInput": data_classes.PartialBidMatrixCalculationInputGraphQL,
        "MultiScenarioPartialBidMatrixCalculationInput": data_classes.MultiScenarioPartialBidMatrixCalculationInputGraphQL,
        "PartialBidMatrixCalculationOutput": data_classes.PartialBidMatrixCalculationOutputGraphQL,
        "BidMatrix": data_classes.BidMatrixGraphQL,
        "MarketConfiguration": data_classes.MarketConfigurationGraphQL,
        "ShopScenario": data_classes.ShopScenarioGraphQL,
        "ShopScenarioSet": data_classes.ShopScenarioSetGraphQL,
        "ShopAttributeMapping": data_classes.ShopAttributeMappingGraphQL,
        "ShopModel": data_classes.ShopModelGraphQL,
        "ShopResult": data_classes.ShopResultGraphQL,
        "ShopCase": data_classes.ShopCaseGraphQL,
        "Alert": data_classes.AlertGraphQL,
        "BidConfigurationDayAhead": data_classes.BidConfigurationDayAheadGraphQL,
        "PriceArea": data_classes.PriceAreaGraphQL,
        "PriceAreaDayAhead": data_classes.PriceAreaDayAheadGraphQL,
        "PriceProduction": data_classes.PriceProductionGraphQL,
        "ShopTimeSeries": data_classes.ShopTimeSeriesGraphQL,
        "ShopCommands": data_classes.ShopCommandsGraphQL,
        "FunctionInput": data_classes.FunctionInputGraphQL,
        "FunctionOutput": data_classes.FunctionOutputGraphQL,
        "PowerAsset": data_classes.PowerAssetGraphQL,
        "PartialBidConfiguration": data_classes.PartialBidConfigurationGraphQL,
        "ShopBasedPartialBidConfiguration": data_classes.ShopBasedPartialBidConfigurationGraphQL,
        "ShopFile": data_classes.ShopFileGraphQL,
        "DateSpecification": data_classes.DateSpecificationGraphQL,
        "ShopOutputTimeSeriesDefinition": data_classes.ShopOutputTimeSeriesDefinitionGraphQL,
        "ShopTimeResolution": data_classes.ShopTimeResolutionGraphQL,
    },
    dm.DataModelId("power_ops_core", "compute_TotalBidMatrixCalculation", "1"): {
        "BidMatrix": data_classes.BidMatrixGraphQL,
        "BidMatrixInformation": data_classes.BidMatrixInformationGraphQL,
        "PartialBidMatrixInformation": data_classes.PartialBidMatrixInformationGraphQL,
        "TotalBidMatrixCalculationInput": data_classes.TotalBidMatrixCalculationInputGraphQL,
        "TotalBidMatrixCalculationOutput": data_classes.TotalBidMatrixCalculationOutputGraphQL,
        "BidDocumentDayAhead": data_classes.BidDocumentDayAheadGraphQL,
        "PriceArea": data_classes.PriceAreaGraphQL,
        "PriceAreaDayAhead": data_classes.PriceAreaDayAheadGraphQL,
        "Alert": data_classes.AlertGraphQL,
        "ShopResult": data_classes.ShopResultGraphQL,
        "MarketConfiguration": data_classes.MarketConfigurationGraphQL,
        "ShopScenario": data_classes.ShopScenarioGraphQL,
        "ShopModel": data_classes.ShopModelGraphQL,
        "ShopAttributeMapping": data_classes.ShopAttributeMappingGraphQL,
        "PriceProduction": data_classes.PriceProductionGraphQL,
        "ShopCase": data_classes.ShopCaseGraphQL,
        "ShopTimeSeries": data_classes.ShopTimeSeriesGraphQL,
        "ShopCommands": data_classes.ShopCommandsGraphQL,
        "FunctionInput": data_classes.FunctionInputGraphQL,
        "FunctionOutput": data_classes.FunctionOutputGraphQL,
        "BidDocument": data_classes.BidDocumentGraphQL,
        "PowerAsset": data_classes.PowerAssetGraphQL,
        "PartialBidConfiguration": data_classes.PartialBidConfigurationGraphQL,
        "BidConfigurationDayAhead": data_classes.BidConfigurationDayAheadGraphQL,
        "ShopFile": data_classes.ShopFileGraphQL,
        "DateSpecification": data_classes.DateSpecificationGraphQL,
        "ShopOutputTimeSeriesDefinition": data_classes.ShopOutputTimeSeriesDefinitionGraphQL,
        "ShopTimeResolution": data_classes.ShopTimeResolutionGraphQL,
    },
    dm.DataModelId("power_ops_core", "compute_WaterValueBasedDayAheadBid", "1"): {
        "TaskDispatcherInput": data_classes.TaskDispatcherInputGraphQL,
        "TaskDispatcherOutput": data_classes.TaskDispatcherOutputGraphQL,
        "PartialBidMatrixCalculationInput": data_classes.PartialBidMatrixCalculationInputGraphQL,
        "WaterValueBasedPartialBidMatrixCalculationInput": data_classes.WaterValueBasedPartialBidMatrixCalculationInputGraphQL,
        "PartialBidMatrixCalculationOutput": data_classes.PartialBidMatrixCalculationOutputGraphQL,
        "Plant": data_classes.PlantGraphQL,
        "PlantWaterValueBased": data_classes.PlantWaterValueBasedGraphQL,
        "Alert": data_classes.AlertGraphQL,
        "BidConfigurationDayAhead": data_classes.BidConfigurationDayAheadGraphQL,
        "PriceArea": data_classes.PriceAreaGraphQL,
        "PriceAreaDayAhead": data_classes.PriceAreaDayAheadGraphQL,
        "MarketConfiguration": data_classes.MarketConfigurationGraphQL,
        "Generator": data_classes.GeneratorGraphQL,
        "FunctionInput": data_classes.FunctionInputGraphQL,
        "FunctionOutput": data_classes.FunctionOutputGraphQL,
        "BidMatrix": data_classes.BidMatrixGraphQL,
        "PowerAsset": data_classes.PowerAssetGraphQL,
        "PartialBidConfiguration": data_classes.PartialBidConfigurationGraphQL,
        "WaterValueBasedPartialBidConfiguration": data_classes.WaterValueBasedPartialBidConfigurationGraphQL,
        "DateSpecification": data_classes.DateSpecificationGraphQL,
        "GeneratorEfficiencyCurve": data_classes.GeneratorEfficiencyCurveGraphQL,
        "TurbineEfficiencyCurve": data_classes.TurbineEfficiencyCurveGraphQL,
    },
    dm.DataModelId("power_ops_core", "config_DayAheadConfiguration", "1"): {
        "BidConfigurationDayAhead": data_classes.BidConfigurationDayAheadGraphQL,
        "MarketConfiguration": data_classes.MarketConfigurationGraphQL,
        "PriceArea": data_classes.PriceAreaGraphQL,
        "PriceAreaDayAhead": data_classes.PriceAreaDayAheadGraphQL,
        "PartialBidConfiguration": data_classes.PartialBidConfigurationGraphQL,
        "ShopBasedPartialBidConfiguration": data_classes.ShopBasedPartialBidConfigurationGraphQL,
        "WaterValueBasedPartialBidConfiguration": data_classes.WaterValueBasedPartialBidConfigurationGraphQL,
        "ShopScenario": data_classes.ShopScenarioGraphQL,
        "ShopScenarioSet": data_classes.ShopScenarioSetGraphQL,
        "ShopAttributeMapping": data_classes.ShopAttributeMappingGraphQL,
        "ShopModel": data_classes.ShopModelGraphQL,
        "Generator": data_classes.GeneratorGraphQL,
        "ShopCommands": data_classes.ShopCommandsGraphQL,
        "PowerAsset": data_classes.PowerAssetGraphQL,
        "Plant": data_classes.PlantGraphQL,
        "PlantInformation": data_classes.PlantInformationGraphQL,
        "PlantWaterValueBased": data_classes.PlantWaterValueBasedGraphQL,
        "DateSpecification": data_classes.DateSpecificationGraphQL,
        "ShopOutputTimeSeriesDefinition": data_classes.ShopOutputTimeSeriesDefinitionGraphQL,
        "ShopFile": data_classes.ShopFileGraphQL,
        "TurbineEfficiencyCurve": data_classes.TurbineEfficiencyCurveGraphQL,
        "GeneratorEfficiencyCurve": data_classes.GeneratorEfficiencyCurveGraphQL,
        "ShopTimeResolution": data_classes.ShopTimeResolutionGraphQL,
    },
    dm.DataModelId("power_ops_core", "frontend_AFRRBid", "1"): {
        "BidDocumentAFRR": data_classes.BidDocumentAFRRGraphQL,
        "BidDocument": data_classes.BidDocumentGraphQL,
        "BidRow": data_classes.BidRowGraphQL,
        "PriceAreaAFRR": data_classes.PriceAreaAFRRGraphQL,
        "PriceArea": data_classes.PriceAreaGraphQL,
        "Alert": data_classes.AlertGraphQL,
        "PowerAsset": data_classes.PowerAssetGraphQL,
    },
    dm.DataModelId("power_ops_core", "frontend_Asset", "1"): {
        "PriceArea": data_classes.PriceAreaGraphQL,
        "PriceAreaAFRR": data_classes.PriceAreaAFRRGraphQL,
        "PriceAreaDayAhead": data_classes.PriceAreaDayAheadGraphQL,
        "PriceAreaInformation": data_classes.PriceAreaInformationGraphQL,
        "BidConfigurationDayAhead": data_classes.BidConfigurationDayAheadGraphQL,
        "MarketConfiguration": data_classes.MarketConfigurationGraphQL,
        "PartialBidConfiguration": data_classes.PartialBidConfigurationGraphQL,
        "PowerAsset": data_classes.PowerAssetGraphQL,
        "Plant": data_classes.PlantGraphQL,
        "PlantWaterValueBased": data_classes.PlantWaterValueBasedGraphQL,
        "PlantInformation": data_classes.PlantInformationGraphQL,
        "Watercourse": data_classes.WatercourseGraphQL,
        "Generator": data_classes.GeneratorGraphQL,
        "DateSpecification": data_classes.DateSpecificationGraphQL,
        "TurbineEfficiencyCurve": data_classes.TurbineEfficiencyCurveGraphQL,
        "GeneratorEfficiencyCurve": data_classes.GeneratorEfficiencyCurveGraphQL,
    },
    dm.DataModelId("power_ops_core", "frontend_DayAheadBid", "1"): {
        "BidDocumentDayAhead": data_classes.BidDocumentDayAheadGraphQL,
        "BidDocument": data_classes.BidDocumentGraphQL,
        "PriceArea": data_classes.PriceAreaGraphQL,
        "PriceAreaDayAhead": data_classes.PriceAreaDayAheadGraphQL,
        "Alert": data_classes.AlertGraphQL,
        "ShopScenario": data_classes.ShopScenarioGraphQL,
        "ShopModel": data_classes.ShopModelGraphQL,
        "ShopAttributeMapping": data_classes.ShopAttributeMappingGraphQL,
        "PriceProduction": data_classes.PriceProductionGraphQL,
        "ShopCommands": data_classes.ShopCommandsGraphQL,
        "ShopCase": data_classes.ShopCaseGraphQL,
        "PowerAsset": data_classes.PowerAssetGraphQL,
        "BidConfigurationDayAhead": data_classes.BidConfigurationDayAheadGraphQL,
        "PartialBidConfiguration": data_classes.PartialBidConfigurationGraphQL,
        "ShopResult": data_classes.ShopResultGraphQL,
        "MarketConfiguration": data_classes.MarketConfigurationGraphQL,
        "ShopTimeSeries": data_classes.ShopTimeSeriesGraphQL,
        "ShopFile": data_classes.ShopFileGraphQL,
        "BidMatrix": data_classes.BidMatrixGraphQL,
        "BidMatrixInformation": data_classes.BidMatrixInformationGraphQL,
        "PartialBidMatrixInformation": data_classes.PartialBidMatrixInformationGraphQL,
        "PartialBidMatrixInformationWithScenarios": data_classes.PartialBidMatrixInformationWithScenariosGraphQL,
        "ShopPenaltyReport": data_classes.ShopPenaltyReportGraphQL,
        "DateSpecification": data_classes.DateSpecificationGraphQL,
        "ShopOutputTimeSeriesDefinition": data_classes.ShopOutputTimeSeriesDefinitionGraphQL,
        "ShopTimeResolution": data_classes.ShopTimeResolutionGraphQL,
    },
    dm.DataModelId("power_ops_core", "compute_BenchmarkingDayAhead", "1"): {
        "BenchmarkingConfigurationDayAhead": data_classes.BenchmarkingConfigurationDayAheadGraphQL,
        "ShopModelWithAssets": data_classes.ShopModelWithAssetsGraphQL,
        "BidConfigurationDayAhead": data_classes.BidConfigurationDayAheadGraphQL,
        "PriceAreaDayAhead": data_classes.PriceAreaDayAheadGraphQL,
        "BenchmarkingTaskDispatcherInputDayAhead": data_classes.BenchmarkingTaskDispatcherInputDayAheadGraphQL,
        "BenchmarkingTaskDispatcherOutputDayAhead": data_classes.BenchmarkingTaskDispatcherOutputDayAheadGraphQL,
        "BenchmarkingShopCase": data_classes.BenchmarkingShopCaseGraphQL,
        "BenchmarkingResultDayAhead": data_classes.BenchmarkingResultDayAheadGraphQL,
        "BenchmarkingProductionObligationDayAhead": data_classes.BenchmarkingProductionObligationDayAheadGraphQL,
        "MarketConfiguration": data_classes.MarketConfigurationGraphQL,
        "BenchmarkingCalculationOutput": data_classes.BenchmarkingCalculationOutputGraphQL,
        "BenchmarkingCalculationInput": data_classes.BenchmarkingCalculationInputGraphQL,
        "Alert": data_classes.AlertGraphQL,
        "ShopScenario": data_classes.ShopScenarioGraphQL,
        "FunctionInput": data_classes.FunctionInputGraphQL,
        "FunctionOutput": data_classes.FunctionOutputGraphQL,
        "PriceArea": data_classes.PriceAreaGraphQL,
        "ShopModel": data_classes.ShopModelGraphQL,
        "ShopCommands": data_classes.ShopCommandsGraphQL,
        "ShopAttributeMapping": data_classes.ShopAttributeMappingGraphQL,
        "PowerAsset": data_classes.PowerAssetGraphQL,
        "PartialBidConfiguration": data_classes.PartialBidConfigurationGraphQL,
        "ShopOutputTimeSeriesDefinition": data_classes.ShopOutputTimeSeriesDefinitionGraphQL,
        "ShopFile": data_classes.ShopFileGraphQL,
        "ShopCase": data_classes.ShopCaseGraphQL,
        "ShopResult": data_classes.ShopResultGraphQL,
        "DateSpecification": data_classes.DateSpecificationGraphQL,
        "ShopTimeSeries": data_classes.ShopTimeSeriesGraphQL,
        "ShopTriggerInput": data_classes.ShopTriggerInputGraphQL,
        "ShopPreprocessorInput": data_classes.ShopPreprocessorInputGraphQL,
        "ShopTimeResolution": data_classes.ShopTimeResolutionGraphQL,
    },
}


T_BaseModel = TypeVar("T_BaseModel", bound=BaseModel)


def instantiate_classes(cls_: type[T_BaseModel], data: list[dict[str, Any]], context: str) -> list[T_BaseModel]:
    if global_config.validate_retrieve is False:
        return [cls_.model_construct(**item) for item in data]

    cls_list = TypeAdapter(list[cls_])  # type: ignore[valid-type]
    try:
        return cls_list.validate_python(data)
    except ValidationError as e:
        failed_count = len({item["loc"][0] for item in e.errors()})
        msg = f"Failed to {context} {cls_.__name__!r}, {failed_count} out of {len(data)} instances failed validation."
        raise PygenValidationError(msg, e) from e


class PygenValidationError(ValueError):
    def __init__(self, message, pydantic_error: ValidationError) -> None:
        super().__init__(message)
        self.errors = pydantic_error.errors()

    def __str__(self):
        return (
            f"{super().__str__()}\nFor details see the ValidationError above."
            "\nHint: You can turn off validation by setting `global_config.validate_retrieve = False` by"
            f" importing `from cognite.powerops.client._generated.v1.config import global_config`."
        )
