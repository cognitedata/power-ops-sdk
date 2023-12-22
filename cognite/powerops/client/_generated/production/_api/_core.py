from __future__ import annotations

import re
from collections import Counter, defaultdict, UserList
from collections.abc import Sequence, Collection
from dataclasses import dataclass, field
from typing import Generic, Literal, Any, Iterator, Protocol, SupportsIndex, TypeVar, overload, cast

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeriesList
from cognite.client.data_classes.data_modeling.instances import Instance
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.production.data_classes._core import (
    DomainModel,
    DomainModelApply,
    DomainRelationApply,
    ResourcesApplyResult,
    T_DomainModel,
    T_DomainModelApply,
    T_DomainModelApplyList,
    T_DomainModelList,
    T_DomainRelation,
    T_DomainRelationApply,
    T_DomainRelationList,
    DomainModelCore,
    DomainRelation,
)

DEFAULT_LIMIT_READ = 25
DEFAULT_QUERY_LIMIT = 3
INSTANCE_QUERY_LIMIT = 1_000
IN_FILTER_LIMIT = 5_000

Aggregations = Literal["avg", "count", "max", "min", "sum"]

_METRIC_AGGREGATIONS_BY_NAME = {
    "avg": dm.aggregations.Avg,
    "count": dm.aggregations.Count,
    "max": dm.aggregations.Max,
    "min": dm.aggregations.Min,
    "sum": dm.aggregations.Sum,
}

_T_co = TypeVar("_T_co", covariant=True)


# Source from https://github.com/python/typing/issues/256#issuecomment-1442633430
# This works because str.__contains__ does not accept an object (either in typeshed or at runtime)
class SequenceNotStr(Protocol[_T_co]):
    @overload
    def __getitem__(self, index: SupportsIndex, /) -> _T_co:
        ...

    @overload
    def __getitem__(self, index: slice, /) -> Sequence[_T_co]:
        ...

    def __contains__(self, value: object, /) -> bool:
        ...

    def __len__(self) -> int:
        ...

    def __iter__(self) -> Iterator[_T_co]:
        ...

    def index(self, value: Any, /, start: int = 0, stop: int = ...) -> int:
        ...

    def count(self, value: Any, /) -> int:
        ...

    def __reversed__(self) -> Iterator[_T_co]:
        ...


class NodeAPI(Generic[T_DomainModel, T_DomainModelApply, T_DomainModelList]):
    def __init__(
        self,
        client: CogniteClient,
        sources: dm.ViewIdentifier | Sequence[dm.ViewIdentifier] | dm.View | Sequence[dm.View],
        class_type: type[T_DomainModel],
        class_apply_type: type[T_DomainModelApply],
        class_list: type[T_DomainModelList],
        class_apply_list: type[T_DomainModelApplyList],
        view_by_write_class: dict[type[DomainModelApply], dm.ViewId],
    ):
        self._client = client
        self._sources = sources
        self._class_type = class_type
        self._class_apply_type = class_apply_type
        self._class_list = class_list
        self._class_apply_list = class_apply_list
        self._view_by_write_class = view_by_write_class

    def _apply(
        self, item: T_DomainModelApply | Sequence[T_DomainModelApply], replace: bool = False
    ) -> ResourcesApplyResult:
        if isinstance(item, DomainModelApply):
            instances = item.to_instances_apply(self._view_by_write_class)
        else:
            instances = self._class_apply_list(item).to_instances_apply(self._view_by_write_class)
        result = self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )
        time_series = []
        if instances.time_series:
            time_series = self._client.time_series.upsert(instances.time_series, mode="patch")

        return ResourcesApplyResult(result.nodes, result.edges, TimeSeriesList(time_series))

    def _delete(self, external_id: str | Sequence[str], space: str) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def _retrieve(
        self,
        external_id: str,
        space: str,
        retrieve_edges: bool = False,
        edge_api_name_type_triple: list[tuple[EdgeAPI, str, dm.DirectRelationReference]] | None = None,
    ) -> T_DomainModel | None:
        ...

    @overload
    def _retrieve(
        self,
        external_id: SequenceNotStr[str],
        space: str,
        retrieve_edges: bool = False,
        edge_api_name_type_triple: list[tuple[EdgeAPI, str, dm.DirectRelationReference]] | None = None,
    ) -> T_DomainModelList:
        ...

    def _retrieve(
        self,
        external_id: str | SequenceNotStr[str],
        space: str,
        retrieve_edges: bool = False,
        edge_api_name_type_triple: list[tuple[EdgeAPI, str, dm.DirectRelationReference]] | None = None,
    ) -> T_DomainModel | T_DomainModelList | None:
        is_multiple = True
        if isinstance(external_id, str):
            node_ids = (space, external_id)
            is_multiple = False
        else:
            node_ids = [(space, ext_id) for ext_id in external_id]

        instances = self._client.data_modeling.instances.retrieve(nodes=node_ids, sources=self._sources)
        nodes = self._class_list([self._class_type.from_instance(node) for node in instances.nodes])

        if retrieve_edges and nodes:
            self._retrieve_and_set_edge_types(nodes, edge_api_name_type_triple)

        if not nodes:
            return None
        elif is_multiple:
            return nodes
        else:
            return nodes[0]

    def _search(
        self,
        view_id: dm.ViewId,
        query: str,
        properties_by_field: dict[str, str],
        properties: str | Sequence[str],
        filter_: dm.Filter | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> T_DomainModelList:
        if isinstance(properties, str):
            properties = [properties]

        if properties:
            properties = [properties_by_field.get(prop, prop) for prop in properties]

        nodes = self._client.data_modeling.instances.search(view_id, query, "node", properties, filter_, limit)
        return self._class_list([self._class_type.from_instance(node) for node in nodes])

    @overload
    def _aggregate(
        self,
        view_id: dm.ViewId,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        properties_by_field: dict[str, str],
        properties: str | Sequence[str] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: str | Sequence[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def _aggregate(
        self,
        view_id: dm.ViewId,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        properties_by_field: dict[str, str],
        properties: str | Sequence[str] = None,
        group_by: str | Sequence[str] | None = None,
        query: str | None = None,
        search_properties: str | Sequence[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def _aggregate(
        self,
        view_id: dm.ViewId,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        properties_by_field: dict[str, str],
        properties: str | Sequence[str] | None = None,
        group_by: str | Sequence[str] | None = None,
        query: str | None = None,
        search_properties: str | Sequence[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        if isinstance(group_by, str):
            group_by = [group_by]

        if group_by:
            group_by = [properties_by_field.get(prop, prop) for prop in group_by]

        if isinstance(search_properties, str):
            search_properties = [search_properties]

        if search_properties:
            search_properties = [properties_by_field.get(prop, prop) for prop in search_properties]

        if isinstance(properties, str):
            properties = [properties]

        if properties:
            properties = [properties_by_field.get(prop, prop) for prop in properties]

        if isinstance(aggregate, (str, dm.aggregations.MetricAggregation)):
            aggregate = [aggregate]

        if properties is None and (invalid := [agg for agg in aggregate if isinstance(agg, str) and agg != "count"]):
            raise ValueError(f"Cannot aggregate on {invalid} without specifying properties")

        aggregates = []
        for agg in aggregate:
            if isinstance(agg, dm.aggregations.MetricAggregation):
                aggregates.append(agg)
            elif isinstance(agg, str):
                if agg == "count" and properties is None:
                    # Special case for count, we just pick the first property
                    first_prop = next(iter(properties_by_field.values()))
                    aggregates.append(dm.aggregations.Count(first_prop))
                elif properties is None:
                    raise ValueError(f"Cannot aggregate on {agg} without specifying properties")
                else:
                    for prop in properties:
                        aggregates.append(_METRIC_AGGREGATIONS_BY_NAME[agg](prop))
            else:
                raise TypeError(f"Expected str or MetricAggregation, got {type(agg)}")

        return self._client.data_modeling.instances.aggregate(
            view=view_id,
            aggregates=aggregates,
            group_by=group_by,
            instance_type="node",
            query=query,
            properties=search_properties,
            filter=filter,
            limit=limit,
        )

    def _histogram(
        self,
        view_id: dm.ViewId,
        property: str,
        interval: float,
        properties_by_field: dict[str, str],
        query: str | None = None,
        search_properties: str | Sequence[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        property = properties_by_field.get(property, property)

        if isinstance(search_properties, str):
            search_properties = [search_properties]
        if search_properties:
            search_properties = [properties_by_field.get(prop, prop) for prop in search_properties]

        return self._client.data_modeling.instances.histogram(
            view=view_id,
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
        filter: dm.Filter,
        retrieve_edges: bool = False,
        edge_api_name_type_triple: list[tuple[EdgeAPI, str, dm.DirectRelationReference]] | None = None,
    ) -> T_DomainModelList:
        nodes = self._client.data_modeling.instances.list("node", sources=self._sources, limit=limit, filter=filter)
        node_list = self._class_list([self._class_type.from_instance(node) for node in nodes])
        if retrieve_edges and node_list:
            self._retrieve_and_set_edge_types(node_list, edge_api_name_type_triple)

        return node_list

    @classmethod
    def _retrieve_and_set_edge_types(
        cls,
        nodes: T_DomainModelList,
        edge_api_name_type_triple: list[tuple[EdgeAPI, str, dm.DirectRelationReference]] | None = None,
    ):
        for edge_api, edge_name, edge_type in edge_api_name_type_triple or []:
            is_type = dm.filters.Equals(
                ["edge", "type"],
                {"space": edge_type.space, "externalId": edge_type.external_id},
            )
            if len(ids := nodes.as_node_ids()) > IN_FILTER_LIMIT:
                edges = edge_api._list(limit=-1, filter_=is_type)
            else:
                is_nodes = dm.filters.In(
                    ["edge", "startNode"],
                    values=[id_.dump(camel_case=True, include_instance_type=False) for id_ in ids],
                )
                edges = edge_api._list(limit=-1, filter_=dm.filters.And(is_type, is_nodes))
            cls._set_edges(nodes, edges, edge_name)

    @staticmethod
    def _set_edges(nodes: Sequence[DomainModel], edges: Sequence[dm.Edge], edge_name: str):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for node in nodes:
            node_id = node.as_tuple_id()
            if node_id in edges_by_start_node:
                setattr(node, edge_name, [edge.end_node.external_id for edge in edges_by_start_node[node_id]])


class EdgeAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def _list(
        self,
        limit: int = DEFAULT_LIMIT_READ,
        filter_: dm.Filter | None = None,
    ) -> dm.EdgeList:
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=filter_)


class EdgePropertyAPI(EdgeAPI, Generic[T_DomainRelation, T_DomainRelationApply, T_DomainRelationList]):
    def __init__(
        self,
        client: CogniteClient,
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId],
        class_type: type[T_DomainRelation],
        class_apply_type: type[T_DomainRelationApply],
        class_list: type[T_DomainRelationList],
    ):
        super().__init__(client)
        self._view_by_write_class = view_by_write_class
        self._view_id = view_by_write_class[class_apply_type]
        self._class_type = class_type
        self._class_apply_type = class_apply_type
        self._class_list = class_list

    def _list(
        self,
        limit: int = DEFAULT_LIMIT_READ,
        filter_: dm.Filter | None = None,
    ) -> T_DomainRelationList:
        edges = self._client.data_modeling.instances.list("edge", limit=limit, filter=filter_, sources=[self._view_id])
        return self._class_list([self._class_type.from_instance(edge) for edge in edges])


@dataclass
class QueryStep:
    # Setup Variables
    name: str
    expression: dm.query.ResultSetExpression
    max_retrieve_limit: int
    select: dm.query.Select
    result_cls: type[DomainModelCore] | None = None

    # Query Variables
    cursor: str | None = None
    total_retrieved: int = 0
    results: list[Instance] = field(default_factory=list)
    last_batch_count: int = 0

    def update_expression_limit(self) -> None:
        if self.max_retrieve_limit == -1:
            self.expression.limit = None
        else:
            self.expression.limit = min(INSTANCE_QUERY_LIMIT, self.max_retrieve_limit - self.total_retrieved)


class QueryBuilder(UserList, Generic[T_DomainModelList]):
    # The unique string is in case the data model has a field that ends with _\d+. This will make sure we don't
    # clean the name of the field.
    _unique_str = "a418"
    _name_pattern = re.compile(r"_a418\d+$")

    def __init__(self, result_cls: type[T_DomainModelList], nodes: Collection[QueryStep] = None):
        super().__init__(nodes or [])
        self._result_cls = result_cls

    # The dunder implementations are to get proper type hints
    def __iter__(self) -> Iterator[QueryStep]:
        return super().__iter__()

    @overload
    def __getitem__(self, item: int) -> QueryStep:
        ...

    @overload
    def __getitem__(self: type[QueryBuilder[T_DomainModelList]], item: slice) -> QueryBuilder[T_DomainModelList]:
        ...

    def __getitem__(self, item: int | slice) -> QueryStep | QueryBuilder[T_DomainModelList]:
        if isinstance(item, slice):
            return self.__class__(self.data[item])
        elif isinstance(item, int):
            return self.data[item]
        else:
            raise TypeError(f"Expected int or slice, got {type(item)}")

    def next_name(self, name: str) -> str:
        counter = Counter(self._clean_name(step.name) for step in self)
        if name in counter:
            return f"{name}_{self._unique_str}{counter[name]}"
        return name

    def _clean_name(self, name: str) -> str:
        return self._name_pattern.sub("", name)

    def reset(self):
        for expression in self:
            expression.total_retrieved = 0
            expression.cursor = None
            expression.results = []

    def update_expression_limits(self) -> None:
        for expression in self:
            expression.update_expression_limit()

    def build(self) -> dm.query.Query:
        with_ = {expression.name: expression.expression for expression in self}
        select = {expression.name: expression.select for expression in self if expression.select}
        cursors = self.cursors

        return dm.query.Query(with_=with_, select=select, cursors=cursors)

    @property
    def cursors(self) -> dict[str, str | None]:
        return {expression.name: expression.cursor for expression in self}

    def update(self, batch: dm.query.QueryResult):
        for expression in self:
            if expression.name not in batch:
                continue
            expression.last_batch_count = len(batch[expression.name])
            expression.total_retrieved += expression.last_batch_count
            expression.cursor = batch.cursors.get(expression.name)
            expression.results.extend(batch[expression.name].data)

    @property
    def is_finished(self):
        return all(
            expression.total_retrieved >= expression.max_retrieve_limit
            or expression.cursor is None
            or expression.last_batch_count == 0
            for expression in self
        )

    def unpack(self) -> T_DomainModelList:
        nodes_by_type: dict[str | None, dict[tuple[str, str], DomainModel]] = defaultdict(dict)
        edges_by_type_by_start_node: dict[tuple[str, str], dict[tuple[str, str], list[dm.Edge]]] = defaultdict(
            lambda: defaultdict(list)
        )
        relation_by_type_by_start_node: dict[
            tuple[str, str], dict[tuple[str, str], list[DomainRelation]]
        ] = defaultdict(lambda: defaultdict(list))
        node_attribute_to_node_type: dict[str, str] = {}

        for step in self:
            name = step.name
            from_ = step.expression.from_

            if isinstance(step.expression, dm.query.NodeResultSetExpression) and from_:
                node_attribute_to_node_type[from_] = name

            if step.result_cls is None:  # This is a data model edge.
                for edge in step.results:
                    edge = cast(dm.Edge, edge)
                    edges_by_type_by_start_node[(from_, name)][
                        (edge.start_node.space, edge.start_node.external_id)
                    ].append(edge)
            elif issubclass(step.result_cls, DomainModel):
                for node in step.results:
                    domain = step.result_cls.from_instance(node)
                    if (id_ := domain.as_tuple_id()) not in nodes_by_type[name]:
                        nodes_by_type[name][id_] = domain
            elif issubclass(step.result_cls, DomainRelation):
                for edge in step.results:
                    domain = step.result_cls.from_instance(edge)
                    relation_by_type_by_start_node[(from_, name)][domain.start_node.as_tuple()].append(domain)

            # Link direct relations
            is_direct_relation = (
                isinstance(step.expression, dm.query.NodeResultSetExpression) and from_ and from_ in nodes_by_type
            )
            if is_direct_relation:
                end_nodes = nodes_by_type[name]
                attribute_name = node_attribute_to_node_type[from_]
                for parent_node in nodes_by_type[from_].values():
                    attribute_value = getattr(parent_node, attribute_name)
                    if isinstance(attribute_value, str):
                        end_id = (parent_node.space, attribute_value)
                    elif isinstance(attribute_value, dm.NodeId):
                        end_id = attribute_value.space, attribute_value.external_id
                    else:
                        continue
                    if end_id in end_nodes:
                        setattr(parent_node, attribute_name, end_nodes[end_id])

        for (node_name, node_attribute), relations_by_start_node in relation_by_type_by_start_node.items():
            for node in nodes_by_type[node_name].values():
                setattr(node, node_attribute, relations_by_start_node.get(node.as_tuple_id(), []))
            for relations in relations_by_start_node.values():
                for relation in relations:
                    edge_name = relation.type.external_id.split(".")[-1]
                    if (nodes := nodes_by_type.get(edge_name)) and (
                        node := nodes.get((relation.end_node.space, relation.end_node.external_id))
                    ):
                        # Relations always have an end node.
                        relation.end_node = node

        for (node_name, node_attribute), edges_by_start_node in edges_by_type_by_start_node.items():
            for node in nodes_by_type[node_name].values():
                edges = edges_by_start_node.get(node.as_tuple_id(), [])
                nodes = nodes_by_type.get(node_attribute_to_node_type.get(node_attribute), {})
                setattr(node, node_attribute, [node for edge in edges if (node := nodes.get(edge.end_node.as_tuple()))])

        return self._result_cls(nodes_by_type[self[0].name].values())


class QueryAPI(Generic[T_DomainModelList]):
    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        view_by_write_class: dict[type[DomainModelApply], dm.ViewId],
    ):
        self._client = client
        self._builder = builder
        self._view_by_write_class = view_by_write_class

    def _query(self) -> T_DomainModelList:
        self._builder.reset()
        query = self._builder.build()

        while True:
            self._builder.update_expression_limits()
            query.cursors = self._builder.cursors
            batch = self._client.data_modeling.instances.query(query)
            self._builder.update(batch)
            if self._builder.is_finished:
                break
        return self._builder.unpack()


def _create_edge_filter(
    edge_type: dm.DirectRelationReference,
    start_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    start_node_space: str = "IntegrationTestsImmutable",
    end_node: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
    space_end_node: str = "IntegrationTestsImmutable",
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
    elif start_node and isinstance(start_node, dm.NodeId):
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
                    {"space": start_node_space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in start_node
                ],
            )
        )
    if end_node and isinstance(end_node, str):
        filters.append(dm.filters.Equals(["edge", "endNode"], value={"space": space_end_node, "externalId": end_node}))
    elif end_node and isinstance(end_node, dm.NodeId):
        filters.append(
            dm.filters.Equals(["edge", "endNode"], value=end_node.dump(camel_case=True, include_instance_type=False))
        )
    if end_node and isinstance(end_node, list):
        filters.append(
            dm.filters.In(
                ["edge", "endNode"],
                values=[
                    {"space": space_end_node, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
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
