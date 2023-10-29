from __future__ import annotations

from collections.abc import Sequence
from typing import Generic, Literal, Optional, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes._core import T_TypeApplyNode, T_TypeNode, T_TypeNodeList

DEFAULT_LIMIT_READ = 25
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


class TypeAPI(Generic[T_TypeNode, T_TypeApplyNode, T_TypeNodeList]):
    def __init__(
        self,
        client: CogniteClient,
        sources: dm.ViewIdentifier | Sequence[dm.ViewIdentifier] | dm.View | Sequence[dm.View],
        class_type: type[T_TypeNode],
        class_apply_type: type[T_TypeApplyNode],
        class_list: type[T_TypeNodeList],
    ):
        self._client = client
        self._sources = sources
        self._class_type = class_type
        self._class_apply_type = class_apply_type
        self._class_list = class_list

    @overload
    def _retrieve(self, external_id: str) -> T_TypeNode:
        ...

    @overload
    def _retrieve(self, external_id: Sequence[str]) -> T_TypeNodeList:
        ...

    def _retrieve(
        self, nodes: dm.NodeId | Sequence[dm.NodeId] | tuple[str, str] | Sequence[tuple[str, str]]
    ) -> T_TypeNode | T_TypeNodeList:
        is_multiple = (
            isinstance(nodes, Sequence)
            and not isinstance(nodes, str)
            and not (isinstance(nodes, tuple) and isinstance(nodes[0], str))
        )
        instances = self._client.data_modeling.instances.retrieve(nodes=nodes, sources=self._sources)
        if is_multiple:
            return self._class_list([self._class_type.from_node(node) for node in instances.nodes])
        return self._class_type.from_node(instances.nodes[0])

    def _search(
        self,
        view_id: dm.ViewId,
        query: str,
        properties_by_field: dict[str, str],
        properties: str | Sequence[str],
        filter_: dm.Filter | None = None,
        limit: int = DEFAULT_LIMIT_READ,
    ) -> T_TypeNodeList:
        if isinstance(properties, str):
            properties = [properties]

        if properties:
            properties = [properties_by_field.get(prop, prop) for prop in properties]

        nodes = self._client.data_modeling.instances.search(view_id, query, "node", properties, filter_, limit)
        return self._class_list([self._class_type.from_node(node) for node in nodes])

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
        properties: Optional[str | Sequence[str]] = None,
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

        result = self._client.data_modeling.instances.aggregate(
            view_id, aggregates, "node", group_by, query, search_properties, filter, limit
        )
        if group_by is None:
            return result[0].aggregates
        return result

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
            view_id, dm.aggregations.Histogram(property, interval), "node", query, search_properties, filter, limit
        )

    def _list(self, limit: int = DEFAULT_LIMIT_READ, filter: dm.Filter | None = None) -> T_TypeNodeList:
        nodes = self._client.data_modeling.instances.list("node", sources=self._sources, limit=limit, filter=filter)
        return self._class_list([self._class_type.from_node(node) for node in nodes])
