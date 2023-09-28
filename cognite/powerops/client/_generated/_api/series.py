from __future__ import annotations

import datetime
from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import Series, SeriesApply, SeriesApplyList, SeriesList

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class SeriesPointsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "Series.points"},
        )
        if isinstance(external_id, str):
            is_series = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_series))

        else:
            is_series_list = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_series_list)
            )

    def list(self, series_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "Series.points"},
        )
        filters.append(is_edge_type)
        if series_id:
            series_ids = [series_id] if isinstance(series_id, str) else series_id
            is_series_list = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in series_ids],
            )
            filters.append(is_series_list)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class SeriesAPI(TypeAPI[Series, SeriesApply, SeriesList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Series,
            class_apply_type=SeriesApply,
            class_list=SeriesList,
        )
        self.view_id = view_id
        self.points = SeriesPointsAPI(client)

    def apply(self, series: SeriesApply | Sequence[SeriesApply], replace: bool = False) -> dm.InstancesApplyResult:
        if isinstance(series, SeriesApply):
            instances = series.to_instances_apply()
        else:
            instances = SeriesApplyList(series).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(SeriesApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(SeriesApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Series:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> SeriesList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Series | SeriesList:
        if isinstance(external_id, str):
            series = self._retrieve((self.sources.space, external_id))

            point_edges = self.points.retrieve(external_id)
            series.points = [edge.end_node.external_id for edge in point_edges]

            return series
        else:
            series_list = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            point_edges = self.points.retrieve(external_id)
            self._set_points(series_list, point_edges)

            return series_list

    def list(
        self,
        min_time_interval_start: datetime.datetime | None = None,
        max_time_interval_start: datetime.datetime | None = None,
        min_time_interval_end: datetime.datetime | None = None,
        max_time_interval_end: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> SeriesList:
        filter_ = _create_filter(
            self.view_id,
            min_time_interval_start,
            max_time_interval_start,
            min_time_interval_end,
            max_time_interval_end,
            external_id_prefix,
            filter,
        )

        series_list = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            point_edges = self.points.list(series_list.as_external_ids(), limit=-1)
            self._set_points(series_list, point_edges)

        return series_list

    @staticmethod
    def _set_points(series_list: Sequence[Series], point_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in point_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for series in series_list:
            node_id = series.id_tuple()
            if node_id in edges_by_start_node:
                series.points = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    min_time_interval_start: datetime.datetime | None = None,
    max_time_interval_start: datetime.datetime | None = None,
    min_time_interval_end: datetime.datetime | None = None,
    max_time_interval_end: datetime.datetime | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_time_interval_start or max_time_interval_start:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("timeIntervalStart"),
                gte=min_time_interval_start.isoformat() if min_time_interval_start else None,
                lte=max_time_interval_start.isoformat() if max_time_interval_start else None,
            )
        )
    if min_time_interval_end or max_time_interval_end:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("timeIntervalEnd"),
                gte=min_time_interval_end.isoformat() if min_time_interval_end else None,
                lte=max_time_interval_end.isoformat() if max_time_interval_end else None,
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
