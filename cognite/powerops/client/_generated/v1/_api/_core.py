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

from cognite.powerops.client._generated.v1 import data_classes
from cognite.powerops.client._generated.v1.data_classes._core import (
    DomainModel,
    DomainModelWrite,
    DEFAULT_INSTANCE_SPACE,
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
    DataClassQueryBuilder,
    NodeQueryStep,
    EdgeQueryStep,
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
        retrieve_edges: bool = False,
        edge_api_name_type_direction_view_id_penta: (
            list[tuple[EdgeAPI, str, dm.DirectRelationReference, Literal["outwards", "inwards"], dm.ViewId]] | None
        ) = None,
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
        if as_child_class:
            if not hasattr(self, "_direct_children_by_external_id"):
                raise ValueError(f"{type(self).__name__} does not have any direct children")
            for child_class_external_id in as_child_class:
                child_cls = self._direct_children_by_external_id.get(child_class_external_id)
                if child_cls is None:
                    raise ValueError(f"Could not find child class with external_id {child_class_external_id}")
                instances = self._client.data_modeling.instances.retrieve(nodes=node_ids, sources=child_cls._view_id)
                items.extend([child_cls.from_instance(node) for node in instances.nodes])
        else:
            instances = self._client.data_modeling.instances.retrieve(nodes=node_ids, sources=self._view_id)
            items.extend([self._class_type.from_instance(node) for node in instances.nodes])

        nodes = self._class_list(items)

        if retrieve_edges and nodes:
            self._retrieve_and_set_edge_types(nodes, edge_api_name_type_direction_view_id_penta)

        if is_multiple:
            return nodes
        elif not nodes:
            return None
        else:
            return nodes[0]

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
        return self._class_list([self._class_type.from_instance(node) for node in nodes])

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

        if isinstance(aggregate, (str, dm.aggregations.MetricAggregation)):
            aggregate = [aggregate]

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
            aggregates=aggregates,
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

    def _list(
        self,
        limit: int,
        filter: dm.Filter | None,
        retrieve_edges: bool = False,
        edge_api_name_type_direction_view_id_penta: (
            list[tuple[EdgeAPI, str, dm.DirectRelationReference, Literal["outwards", "inwards"], dm.ViewId]] | None
        ) = None,
        sort_by: str | list[str] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> T_DomainModelList:
        sort_input = self._create_sort(sort_by, direction, sort)
        nodes = self._client.data_modeling.instances.list(
            instance_type="node",
            sources=self._view_id,
            limit=limit,
            filter=filter,
            sort=sort_input,
        )
        node_list = self._class_list([self._class_type.from_instance(node) for node in nodes])
        if retrieve_edges and node_list:
            self._retrieve_and_set_edge_types(node_list, edge_api_name_type_direction_view_id_penta)  # type: ignore[arg-type]

        return node_list

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

    def _retrieve_and_set_edge_types(
        self,
        nodes: T_DomainModelList,  # type: ignore[misc]
        edge_api_name_type_direction_view_id_penta: (
            list[tuple[EdgeAPI, str, dm.DirectRelationReference, Literal["outwards", "inwards"], dm.ViewId]] | None
        ) = None,
    ):
        filter_: dm.Filter | None
        for edge_type, values in groupby(edge_api_name_type_direction_view_id_penta or [], lambda x: x[2].as_tuple()):
            edges: dict[dm.EdgeId, dm.Edge] = {}
            value_list = list(values)
            for edge_api, edge_name, edge_type, direction, view_id in value_list:
                is_type = dm.filters.Equals(
                    ["edge", "type"],
                    {"space": edge_type.space, "externalId": edge_type.external_id},
                )
                if len(ids := nodes.as_node_ids()) > IN_FILTER_LIMIT:
                    filter_ = is_type
                else:
                    is_nodes = dm.filters.In(
                        ["edge", "startNode"] if direction == "outwards" else ["edge", "endNode"],
                        values=[id_.dump(camel_case=True, include_instance_type=False) for id_ in ids],
                    )
                    filter_ = dm.filters.And(is_type, is_nodes)
                result = edge_api._list(limit=-1, filter_=filter_)
                edges.update({edge.as_id(): edge for edge in result})
            edge_list = list(edges.values())
            if len(value_list) == 1:
                _, edge_name, _, direction, _ = value_list[0]
                self._set_edges(nodes, edge_list, edge_name, direction)
            else:
                # This is an 'edge' case where we have view with multiple edges of the same type.
                edge_by_end_node: dict[tuple[str, str], list[dm.Edge]] = defaultdict(list)
                for edge in edge_list:
                    node_id = edge.end_node.as_tuple() if direction == "outwards" else edge.start_node.as_tuple()
                    edge_by_end_node[node_id].append(edge)

                for no, (edge_api, edge_name, _, direction, view_id) in enumerate(value_list):
                    if not edge_by_end_node:
                        break
                    if no == len(value_list) - 1:
                        # Last edge, use all remaining nodes
                        attribute_edges = [e for e_list in edge_by_end_node.values() for e in e_list]
                    else:
                        existing = self._client.data_modeling.instances.retrieve(
                            nodes=list(edge_by_end_node), sources=view_id
                        )
                        attribute_edges = []
                        for node in existing.nodes:
                            attribute_edge = edge_by_end_node.pop(node.as_id().as_tuple(), [])
                            attribute_edges.extend(attribute_edge)

                    self._set_edges(nodes, attribute_edges, edge_name, direction)

    @staticmethod
    def _set_edges(
        nodes: Sequence[DomainModel],
        edges: Sequence[dm.Edge],
        edge_name: str,
        direction: Literal["outwards", "inwards"],
    ):
        edges_by_node: dict[tuple, list] = defaultdict(list)
        for edge in edges:
            node_id = edge.start_node.as_tuple() if direction == "outwards" else edge.end_node.as_tuple()
            edges_by_node[node_id].append(edge)

        for node in nodes:
            node_id = node.as_tuple_id()
            if node_id in edges_by_node:
                setattr(
                    node,
                    edge_name,
                    [
                        edge.end_node.external_id if direction == "outwards" else edge.start_node.external_id
                        for edge in edges_by_node[node_id]
                    ],
                )


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


class EdgeAPI(ABC):
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
        return self._class_list([self._class_type.from_instance(edge) for edge in edges])  # type: ignore[misc]


class QueryAPI(Generic[T_DomainModelList]):
    def __init__(
        self,
        client: CogniteClient,
        builder: DataClassQueryBuilder[T_DomainModelList],
    ):
        self._client = client
        self._builder = builder

    def _query(self) -> T_DomainModelList:
        self._builder.execute_query(self._client, remove_not_connected=True)
        return self._builder.unpack()


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
    if space and isinstance(space, list):
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
        _, data = list(response.items())[0]
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
                raise ValueError(f"Could not find class for type {data['__typename']}")
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
        "GeneratorEfficiencyCurve": data_classes.GeneratorEfficiencyCurveGraphQL,
        "TurbineEfficiencyCurve": data_classes.TurbineEfficiencyCurveGraphQL,
        "FunctionInput": data_classes.FunctionInputGraphQL,
        "FunctionOutput": data_classes.FunctionOutputGraphQL,
        "BidMatrix": data_classes.BidMatrixGraphQL,
        "PowerAsset": data_classes.PowerAssetGraphQL,
        "PartialBidConfiguration": data_classes.PartialBidConfigurationGraphQL,
        "WaterValueBasedPartialBidConfiguration": data_classes.WaterValueBasedPartialBidConfigurationGraphQL,
        "DateSpecification": data_classes.DateSpecificationGraphQL,
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
        "TurbineEfficiencyCurve": data_classes.TurbineEfficiencyCurveGraphQL,
        "GeneratorEfficiencyCurve": data_classes.GeneratorEfficiencyCurveGraphQL,
        "ShopCommands": data_classes.ShopCommandsGraphQL,
        "PowerAsset": data_classes.PowerAssetGraphQL,
        "Plant": data_classes.PlantGraphQL,
        "PlantInformation": data_classes.PlantInformationGraphQL,
        "PlantWaterValueBased": data_classes.PlantWaterValueBasedGraphQL,
        "DateSpecification": data_classes.DateSpecificationGraphQL,
        "ShopOutputTimeSeriesDefinition": data_classes.ShopOutputTimeSeriesDefinitionGraphQL,
        "ShopFile": data_classes.ShopFileGraphQL,
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
        "TurbineEfficiencyCurve": data_classes.TurbineEfficiencyCurveGraphQL,
        "GeneratorEfficiencyCurve": data_classes.GeneratorEfficiencyCurveGraphQL,
        "DateSpecification": data_classes.DateSpecificationGraphQL,
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
