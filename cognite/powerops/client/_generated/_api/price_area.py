from __future__ import annotations

import datetime
import warnings
from collections import defaultdict
from collections.abc import Sequence
from typing import Literal, overload

import pandas as pd
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import Datapoints, DatapointsArrayList, DatapointsList, TimeSeriesList
from cognite.client.data_classes.datapoints import Aggregate

from cognite.powerops.client._generated.data_classes import PriceArea, PriceAreaApply, PriceAreaApplyList, PriceAreaList

from ._core import DEFAULT_LIMIT_READ, INSTANCE_QUERY_LIMIT, TypeAPI

ColumnNames = Literal["name", "description", "dayAheadPrice"]


class PriceAreaDayAheadPriceQuery:
    def __init__(
        self,
        client: CogniteClient,
        view_id: dm.ViewId,
        timeseries_limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ):
        self._client = client
        self._view_id = view_id
        self._timeseries_limit = timeseries_limit
        self._filter = filter

    def retrieve(
        self,
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        *,
        aggregates: Aggregate | list[Aggregate] | None = None,
        granularity: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
    ) -> DatapointsList:
        external_ids = self._retrieve_timeseries_external_ids_with_extra()
        if external_ids:
            return self._client.time_series.data.retrieve(
                external_id=list(external_ids),
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                limit=limit,
                include_outside_points=include_outside_points,
            )
        else:
            return DatapointsList([])

    def retrieve_arrays(
        self,
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        *,
        aggregates: Aggregate | list[Aggregate] | None = None,
        granularity: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
    ) -> DatapointsArrayList:
        external_ids = self._retrieve_timeseries_external_ids_with_extra()
        if external_ids:
            return self._client.time_series.data.retrieve_arrays(
                external_id=list(external_ids),
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                limit=limit,
                include_outside_points=include_outside_points,
            )
        else:
            return DatapointsArrayList([])

    def retrieve_dataframe(
        self,
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        *,
        aggregates: Aggregate | list[Aggregate] | None = None,
        granularity: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        column_names: ColumnNames | list[ColumnNames] = "dayAheadPrice",
    ) -> pd.DataFrame:
        external_ids = self._retrieve_timeseries_external_ids_with_extra(column_names)
        if external_ids:
            df = self._client.time_series.data.retrieve_dataframe(
                external_id=list(external_ids),
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                limit=limit,
                include_outside_points=include_outside_points,
                uniform_index=uniform_index,
                include_aggregate_name=include_aggregate_name,
                include_granularity_name=include_granularity_name,
            )
            is_aggregate = aggregates is not None
            return self._rename_columns(
                external_ids,
                df,
                column_names,
                is_aggregate and include_aggregate_name,
                is_aggregate and include_granularity_name,
            )
        else:
            return pd.DataFrame()

    def retrieve_dataframe_in_tz(
        self,
        start: datetime.datetime,
        end: datetime.datetime,
        *,
        aggregates: Aggregate | Sequence[Aggregate] | None = None,
        granularity: str | None = None,
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        column_names: ColumnNames | list[ColumnNames] = "dayAheadPrice",
    ) -> pd.DataFrame:
        external_ids = self._retrieve_timeseries_external_ids_with_extra(column_names)
        if external_ids:
            df = self._client.time_series.data.retrieve_dataframe_in_tz(
                external_id=list(external_ids),
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                uniform_index=uniform_index,
                include_aggregate_name=include_aggregate_name,
                include_granularity_name=include_granularity_name,
            )
            is_aggregate = aggregates is not None
            return self._rename_columns(
                external_ids,
                df,
                column_names,
                is_aggregate and include_aggregate_name,
                is_aggregate and include_granularity_name,
            )
        else:
            return pd.DataFrame()

    def retrieve_latest(
        self,
        before: None | int | str | datetime.datetime = None,
    ) -> Datapoints | DatapointsList | None:
        external_ids = self._retrieve_timeseries_external_ids_with_extra()
        if external_ids:
            return self._client.time_series.data.retrieve_latest(
                external_id=list(external_ids),
                before=before,
            )
        else:
            return None

    def plot(
        self,
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        *,
        aggregates: Aggregate | Sequence[Aggregate] | None = None,
        granularity: str | None = None,
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        column_names: ColumnNames | list[ColumnNames] = "dayAheadPrice",
        warning: bool = True,
        **kwargs,
    ) -> None:
        if warning:
            warnings.warn(
                "This methods if an experiment and might be removed in the future without notice.", stacklevel=2
            )
        if all(isinstance(time, datetime.datetime) and time.tzinfo is not None for time in [start, end]):
            df = self.retrieve_dataframe_in_tz(
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                uniform_index=uniform_index,
                include_aggregate_name=include_aggregate_name,
                include_granularity_name=include_granularity_name,
                column_names=column_names,
            )
        else:
            df = self.retrieve_dataframe(
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                uniform_index=uniform_index,
                include_aggregate_name=include_aggregate_name,
                include_granularity_name=include_granularity_name,
                column_names=column_names,
            )
        df.plot(**kwargs)

    def _retrieve_timeseries_external_ids_with_extra(
        self, extra_properties: ColumnNames | list[ColumnNames] = "dayAheadPrice"
    ) -> dict[str, list[str]]:
        return _retrieve_timeseries_external_ids_with_extra_day_ahead_price(
            self._client,
            self._view_id,
            self._filter,
            self._timeseries_limit,
            extra_properties,
        )

    @staticmethod
    def _rename_columns(
        external_ids: dict[str, list[str]],
        df: pd.DataFrame,
        column_names: ColumnNames | list[ColumnNames],
        include_aggregate_name: bool,
        include_granularity_name: bool,
    ) -> pd.DataFrame:
        if isinstance(column_names, str) and column_names == "dayAheadPrice":
            return df
        splits = sum(included for included in [include_aggregate_name, include_granularity_name])
        if splits == 0:
            df.columns = ["-".join(external_ids[external_id]) for external_id in df.columns]
        else:
            column_parts = (col.rsplit("|", maxsplit=splits) for col in df.columns)
            df.columns = [
                "-".join(external_ids[external_id]) + "|" + "|".join(parts) for external_id, *parts in column_parts
            ]
        return df


class PriceAreaDayAheadPriceAPI:
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        self._client = client
        self._view_id = view_id

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PriceAreaDayAheadPriceQuery:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            description,
            description_prefix,
            external_id_prefix,
            filter,
        )

        return PriceAreaDayAheadPriceQuery(
            client=self._client,
            view_id=self._view_id,
            timeseries_limit=limit,
            filter=filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TimeSeriesList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            description,
            description_prefix,
            external_id_prefix,
            filter,
        )
        external_ids = _retrieve_timeseries_external_ids_with_extra_day_ahead_price(
            self._client, self._view_id, filter_, limit
        )
        if external_ids:
            return self._client.time_series.retrieve_multiple(external_ids=list(external_ids))
        else:
            return TimeSeriesList([])


def _retrieve_timeseries_external_ids_with_extra_day_ahead_price(
    client: CogniteClient,
    view_id: dm.ViewId,
    filter_: dm.Filter | None,
    limit: int,
    extra_properties: ColumnNames | list[ColumnNames] = "dayAheadPrice",
) -> dict[str, list[str]]:
    properties = ["dayAheadPrice"]
    if extra_properties == "dayAheadPrice":
        ...
    elif isinstance(extra_properties, str) and extra_properties != "dayAheadPrice":
        properties.append(extra_properties)
    elif isinstance(extra_properties, list):
        properties.extend([prop for prop in extra_properties if prop != "dayAheadPrice"])
    else:
        raise ValueError(f"Invalid value for extra_properties: {extra_properties}")

    if isinstance(extra_properties, str):
        extra_list = [extra_properties]
    else:
        extra_list = extra_properties
    has_data = dm.filters.HasData([dm.ContainerId("power-ops", "PriceArea")], [view_id])
    filter_ = dm.filters.And(filter_, has_data) if filter_ else has_data

    cursor = None
    external_ids: dict[str, list[str]] = {}
    total_retrieved = 0
    while True:
        query_limit = min(INSTANCE_QUERY_LIMIT, limit - total_retrieved)
        selected_nodes = dm.query.NodeResultSetExpression(filter=filter_, limit=query_limit)
        query = dm.query.Query(
            with_={
                "nodes": selected_nodes,
            },
            select={
                "nodes": dm.query.Select(
                    [dm.query.SourceSelector(view_id, properties)],
                )
            },
            cursors={"nodes": cursor},
        )
        result = client.data_modeling.instances.query(query)
        batch_external_ids = {
            node.properties[view_id]["dayAheadPrice"]: [node.properties[view_id].get(prop, "") for prop in extra_list]
            for node in result.data["nodes"].data
        }
        total_retrieved += len(batch_external_ids)
        external_ids.update(batch_external_ids)
        cursor = result.cursors["nodes"]
        if total_retrieved >= limit or cursor is None:
            break
    return external_ids


class PriceAreaPlantsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "PriceArea.plants"},
        )
        if isinstance(external_id, str):
            is_price_area = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_price_area)
            )

        else:
            is_price_areas = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_price_areas)
            )

    def list(self, price_area_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "PriceArea.plants"},
        )
        filters.append(is_edge_type)
        if price_area_id:
            price_area_ids = [price_area_id] if isinstance(price_area_id, str) else price_area_id
            is_price_areas = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in price_area_ids],
            )
            filters.append(is_price_areas)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class PriceAreaWatercoursesAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "PriceArea.watercourses"},
        )
        if isinstance(external_id, str):
            is_price_area = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_price_area)
            )

        else:
            is_price_areas = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_price_areas)
            )

    def list(self, price_area_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "PriceArea.watercourses"},
        )
        filters.append(is_edge_type)
        if price_area_id:
            price_area_ids = [price_area_id] if isinstance(price_area_id, str) else price_area_id
            is_price_areas = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in price_area_ids],
            )
            filters.append(is_price_areas)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class PriceAreaAPI(TypeAPI[PriceArea, PriceAreaApply, PriceAreaList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=PriceArea,
            class_apply_type=PriceAreaApply,
            class_list=PriceAreaList,
        )
        self.view_id = view_id
        self.plants = PriceAreaPlantsAPI(client)
        self.watercourses = PriceAreaWatercoursesAPI(client)
        self.day_ahead_price = PriceAreaDayAheadPriceAPI(client, view_id)

    def apply(
        self, price_area: PriceAreaApply | Sequence[PriceAreaApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(price_area, PriceAreaApply):
            instances = price_area.to_instances_apply()
        else:
            instances = PriceAreaApplyList(price_area).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(PriceAreaApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(PriceAreaApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> PriceArea:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> PriceAreaList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> PriceArea | PriceAreaList:
        if isinstance(external_id, str):
            price_area = self._retrieve((self.sources.space, external_id))

            plant_edges = self.plants.retrieve(external_id)
            price_area.plants = [edge.end_node.external_id for edge in plant_edges]
            watercourse_edges = self.watercourses.retrieve(external_id)
            price_area.watercourses = [edge.end_node.external_id for edge in watercourse_edges]

            return price_area
        else:
            price_areas = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            plant_edges = self.plants.retrieve(external_id)
            self._set_plants(price_areas, plant_edges)
            watercourse_edges = self.watercourses.retrieve(external_id)
            self._set_watercourses(price_areas, watercourse_edges)

            return price_areas

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> PriceAreaList:
        filter_ = _create_filter(
            self.view_id,
            name,
            name_prefix,
            description,
            description_prefix,
            external_id_prefix,
            filter,
        )

        price_areas = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            plant_edges = self.plants.list(price_areas.as_external_ids(), limit=-1)
            self._set_plants(price_areas, plant_edges)
            watercourse_edges = self.watercourses.list(price_areas.as_external_ids(), limit=-1)
            self._set_watercourses(price_areas, watercourse_edges)

        return price_areas

    @staticmethod
    def _set_plants(price_areas: Sequence[PriceArea], plant_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in plant_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for price_area in price_areas:
            node_id = price_area.id_tuple()
            if node_id in edges_by_start_node:
                price_area.plants = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_watercourses(price_areas: Sequence[PriceArea], watercourse_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in watercourse_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for price_area in price_areas:
            node_id = price_area.id_tuple()
            if node_id in edges_by_start_node:
                price_area.watercourses = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
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
    if description and isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
