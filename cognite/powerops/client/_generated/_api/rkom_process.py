from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    RKOMProcess,
    RKOMProcessApply,
    RKOMProcessApplyList,
    RKOMProcessFields,
    RKOMProcessList,
    RKOMProcessTextFields,
)
from cognite.powerops.client._generated.data_classes._rkom_process import _RKOMPROCESS_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, IN_FILTER_LIMIT, Aggregations, TypeAPI


class RKOMProcessIncrementalMappingsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="power-ops") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "RKOMProcess.incremental_mappings"},
        )
        if isinstance(external_id, str):
            is_rkom_proces = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_rkom_proces)
            )

        else:
            is_rkom_process = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_rkom_process)
            )

    def list(
        self, rkom_proces_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="power-ops"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "RKOMProcess.incremental_mappings"},
        )
        filters.append(is_edge_type)
        if rkom_proces_id:
            rkom_proces_ids = [rkom_proces_id] if isinstance(rkom_proces_id, str) else rkom_proces_id
            is_rkom_process = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in rkom_proces_ids],
            )
            filters.append(is_rkom_process)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class RKOMProcessAPI(TypeAPI[RKOMProcess, RKOMProcessApply, RKOMProcessList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=RKOMProcess,
            class_apply_type=RKOMProcessApply,
            class_list=RKOMProcessList,
        )
        self._view_id = view_id
        self.incremental_mappings = RKOMProcessIncrementalMappingsAPI(client)

    def apply(
        self, rkom_proces: RKOMProcessApply | Sequence[RKOMProcessApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(rkom_proces, RKOMProcessApply):
            instances = rkom_proces.to_instances_apply()
        else:
            instances = RKOMProcessApplyList(rkom_proces).to_instances_apply()
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
    def retrieve(self, external_id: str) -> RKOMProcess:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> RKOMProcessList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> RKOMProcess | RKOMProcessList:
        if isinstance(external_id, str):
            rkom_proces = self._retrieve((self._sources.space, external_id))

            incremental_mapping_edges = self.incremental_mappings.retrieve(external_id)
            rkom_proces.incremental_mappings = [edge.end_node.external_id for edge in incremental_mapping_edges]

            return rkom_proces
        else:
            rkom_process = self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

            incremental_mapping_edges = self.incremental_mappings.retrieve(external_id)
            self._set_incremental_mappings(rkom_process, incremental_mapping_edges)

            return rkom_process

    def search(
        self,
        query: str,
        properties: RKOMProcessTextFields | Sequence[RKOMProcessTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        shop: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> RKOMProcessList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            bid,
            shop,
            timezone,
            timezone_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _RKOMPROCESS_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: RKOMProcessFields | Sequence[RKOMProcessFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: RKOMProcessTextFields | Sequence[RKOMProcessTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        shop: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
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
        property: RKOMProcessFields | Sequence[RKOMProcessFields] | None = None,
        group_by: RKOMProcessFields | Sequence[RKOMProcessFields] = None,
        query: str | None = None,
        search_properties: RKOMProcessTextFields | Sequence[RKOMProcessTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        shop: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
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
        property: RKOMProcessFields | Sequence[RKOMProcessFields] | None = None,
        group_by: RKOMProcessFields | Sequence[RKOMProcessFields] | None = None,
        query: str | None = None,
        search_property: RKOMProcessTextFields | Sequence[RKOMProcessTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        shop: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
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
            timezone,
            timezone_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _RKOMPROCESS_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: RKOMProcessFields,
        interval: float,
        query: str | None = None,
        search_property: RKOMProcessTextFields | Sequence[RKOMProcessTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        shop: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
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
            timezone,
            timezone_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _RKOMPROCESS_PROPERTIES_BY_FIELD,
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
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> RKOMProcessList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            bid,
            shop,
            timezone,
            timezone_prefix,
            external_id_prefix,
            filter,
        )

        rkom_process = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := rkom_process.as_external_ids()) > IN_FILTER_LIMIT:
                incremental_mapping_edges = self.incremental_mappings.list(limit=-1)
            else:
                incremental_mapping_edges = self.incremental_mappings.list(external_ids, limit=-1)
            self._set_incremental_mappings(rkom_process, incremental_mapping_edges)

        return rkom_process

    @staticmethod
    def _set_incremental_mappings(rkom_process: Sequence[RKOMProcess], incremental_mapping_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in incremental_mapping_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for rkom_proces in rkom_process:
            node_id = rkom_proces.id_tuple()
            if node_id in edges_by_start_node:
                rkom_proces.incremental_mappings = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    shop: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    timezone: str | list[str] | None = None,
    timezone_prefix: str | None = None,
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
    if timezone and isinstance(timezone, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timezone"), value=timezone))
    if timezone and isinstance(timezone, list):
        filters.append(dm.filters.In(view_id.as_property_ref("timezone"), values=timezone))
    if timezone_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("timezone"), value=timezone_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
