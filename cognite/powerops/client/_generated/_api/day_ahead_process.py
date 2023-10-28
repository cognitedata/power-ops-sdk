from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    DayAheadProcess,
    DayAheadProcessApply,
    DayAheadProcessApplyList,
    DayAheadProcessFields,
    DayAheadProcessList,
    DayAheadProcessTextFields,
)
from cognite.powerops.client._generated.data_classes._day_ahead_process import _DAYAHEADPROCESS_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, IN_FILTER_LIMIT, Aggregations, TypeAPI


class DayAheadProcessIncrementalMappingsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="power-ops") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "DayAheadProcess.incremental_mappings"},
        )
        if isinstance(external_id, str):
            is_day_ahead_proces = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_day_ahead_proces)
            )

        else:
            is_day_ahead_process = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_day_ahead_process)
            )

    def list(
        self, day_ahead_proces_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="power-ops"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "DayAheadProcess.incremental_mappings"},
        )
        filters.append(is_edge_type)
        if day_ahead_proces_id:
            day_ahead_proces_ids = (
                [day_ahead_proces_id] if isinstance(day_ahead_proces_id, str) else day_ahead_proces_id
            )
            is_day_ahead_process = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in day_ahead_proces_ids],
            )
            filters.append(is_day_ahead_process)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class DayAheadProcessBidMatrixGeneratorConfigAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="power-ops") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "DayAheadProcess.bidMatrixGeneratorConfig"},
        )
        if isinstance(external_id, str):
            is_day_ahead_proces = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_day_ahead_proces)
            )

        else:
            is_day_ahead_process = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_day_ahead_process)
            )

    def list(
        self, day_ahead_proces_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="power-ops"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "DayAheadProcess.bidMatrixGeneratorConfig"},
        )
        filters.append(is_edge_type)
        if day_ahead_proces_id:
            day_ahead_proces_ids = (
                [day_ahead_proces_id] if isinstance(day_ahead_proces_id, str) else day_ahead_proces_id
            )
            is_day_ahead_process = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in day_ahead_proces_ids],
            )
            filters.append(is_day_ahead_process)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class DayAheadProcessAPI(TypeAPI[DayAheadProcess, DayAheadProcessApply, DayAheadProcessList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=DayAheadProcess,
            class_apply_type=DayAheadProcessApply,
            class_list=DayAheadProcessList,
        )
        self._view_id = view_id
        self.incremental_mappings = DayAheadProcessIncrementalMappingsAPI(client)
        self.bid_matrix_generator_config = DayAheadProcessBidMatrixGeneratorConfigAPI(client)

    def apply(
        self, day_ahead_proces: DayAheadProcessApply | Sequence[DayAheadProcessApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(day_ahead_proces, DayAheadProcessApply):
            instances = day_ahead_proces.to_instances_apply()
        else:
            instances = DayAheadProcessApplyList(day_ahead_proces).to_instances_apply()
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space="power-ops") -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> DayAheadProcess:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> DayAheadProcessList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> DayAheadProcess | DayAheadProcessList:
        if isinstance(external_id, str):
            day_ahead_proces = self._retrieve((self._sources.space, external_id))

            incremental_mapping_edges = self.incremental_mappings.retrieve(external_id)
            day_ahead_proces.incremental_mappings = [edge.end_node.external_id for edge in incremental_mapping_edges]
            bid_matrix_generator_config_edges = self.bid_matrix_generator_config.retrieve(external_id)
            day_ahead_proces.bid_matrix_generator_config = [
                edge.end_node.external_id for edge in bid_matrix_generator_config_edges
            ]

            return day_ahead_proces
        else:
            day_ahead_process = self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

            incremental_mapping_edges = self.incremental_mappings.retrieve(external_id)
            self._set_incremental_mappings(day_ahead_process, incremental_mapping_edges)
            bid_matrix_generator_config_edges = self.bid_matrix_generator_config.retrieve(external_id)
            self._set_bid_matrix_generator_config(day_ahead_process, bid_matrix_generator_config_edges)

            return day_ahead_process

    def search(
        self,
        query: str,
        properties: DayAheadProcessTextFields | Sequence[DayAheadProcessTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        shop: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> DayAheadProcessList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            bid,
            shop,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _DAYAHEADPROCESS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: DayAheadProcessFields | Sequence[DayAheadProcessFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: DayAheadProcessTextFields | Sequence[DayAheadProcessTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        shop: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: DayAheadProcessFields | Sequence[DayAheadProcessFields] | None = None,
        group_by: DayAheadProcessFields | Sequence[DayAheadProcessFields] = None,
        query: str | None = None,
        search_properties: DayAheadProcessTextFields | Sequence[DayAheadProcessTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        shop: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: DayAheadProcessFields | Sequence[DayAheadProcessFields] | None = None,
        group_by: DayAheadProcessFields | Sequence[DayAheadProcessFields] | None = None,
        query: str | None = None,
        search_property: DayAheadProcessTextFields | Sequence[DayAheadProcessTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        shop: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            bid,
            shop,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _DAYAHEADPROCESS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: DayAheadProcessFields,
        interval: float,
        query: str | None = None,
        search_property: DayAheadProcessTextFields | Sequence[DayAheadProcessTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        shop: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            bid,
            shop,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _DAYAHEADPROCESS_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        shop: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> DayAheadProcessList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            bid,
            shop,
            external_id_prefix,
            filter,
        )

        day_ahead_process = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := day_ahead_process.as_external_ids()) > IN_FILTER_LIMIT:
                incremental_mapping_edges = self.incremental_mappings.list(limit=-1)
            else:
                incremental_mapping_edges = self.incremental_mappings.list(external_ids, limit=-1)
            self._set_incremental_mappings(day_ahead_process, incremental_mapping_edges)
            if len(external_ids := day_ahead_process.as_external_ids()) > IN_FILTER_LIMIT:
                bid_matrix_generator_config_edges = self.bid_matrix_generator_config.list(limit=-1)
            else:
                bid_matrix_generator_config_edges = self.bid_matrix_generator_config.list(external_ids, limit=-1)
            self._set_bid_matrix_generator_config(day_ahead_process, bid_matrix_generator_config_edges)

        return day_ahead_process

    @staticmethod
    def _set_incremental_mappings(
        day_ahead_process: Sequence[DayAheadProcess], incremental_mapping_edges: Sequence[dm.Edge]
    ):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in incremental_mapping_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for day_ahead_proces in day_ahead_process:
            node_id = day_ahead_proces.id_tuple()
            if node_id in edges_by_start_node:
                day_ahead_proces.incremental_mappings = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]

    @staticmethod
    def _set_bid_matrix_generator_config(
        day_ahead_process: Sequence[DayAheadProcess], bid_matrix_generator_config_edges: Sequence[dm.Edge]
    ):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in bid_matrix_generator_config_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for day_ahead_proces in day_ahead_process:
            node_id = day_ahead_proces.id_tuple()
            if node_id in edges_by_start_node:
                day_ahead_proces.bid_matrix_generator_config = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]


def _create_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    shop: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if bid and isinstance(bid, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("bid"), value={"space": "power-ops", "externalId": bid})
        )
    if bid and isinstance(bid, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bid"), value={"space": bid[0], "externalId": bid[1]}))
    if bid and isinstance(bid, list) and isinstance(bid[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("bid"), values=[{"space": "power-ops", "externalId": item} for item in bid]
            )
        )
    if bid and isinstance(bid, list) and isinstance(bid[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("bid"), values=[{"space": item[0], "externalId": item[1]} for item in bid]
            )
        )
    if shop and isinstance(shop, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("shop"), value={"space": "power-ops", "externalId": shop})
        )
    if shop and isinstance(shop, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("shop"), value={"space": shop[0], "externalId": shop[1]})
        )
    if shop and isinstance(shop, list) and isinstance(shop[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("shop"), values=[{"space": "power-ops", "externalId": item} for item in shop]
            )
        )
    if shop and isinstance(shop, list) and isinstance(shop[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("shop"), values=[{"space": item[0], "externalId": item[1]} for item in shop]
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
