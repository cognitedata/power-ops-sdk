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
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList
from cognite.client.data_classes.datapoints import Aggregate

from cognite.powerops.client._generated.data_classes import (
    InputTimeSeriesMapping,
    InputTimeSeriesMappingApply,
    InputTimeSeriesMappingApplyList,
    InputTimeSeriesMappingFields,
    InputTimeSeriesMappingList,
    InputTimeSeriesMappingTextFields,
)
from cognite.powerops.client._generated.data_classes._input_time_series_mapping import (
    _INPUTTIMESERIESMAPPING_PROPERTIES_BY_FIELD,
)

from ._core import DEFAULT_LIMIT_READ, IN_FILTER_LIMIT, INSTANCE_QUERY_LIMIT, Aggregations, TypeAPI

ColumnNames = Literal[
    "shopObjectType", "shopObjectName", "shopAttributeName", "cdfTimeSeries", "retrieve", "aggregation"
]


class InputTimeSeriesMappingCdfTimeSeriesQuery:
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
        column_names: ColumnNames | list[ColumnNames] = "cdfTimeSeries",
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
        column_names: ColumnNames | list[ColumnNames] = "cdfTimeSeries",
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
        column_names: ColumnNames | list[ColumnNames] = "cdfTimeSeries",
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
        self, extra_properties: ColumnNames | list[ColumnNames] = "cdfTimeSeries"
    ) -> dict[str, list[str]]:
        return _retrieve_timeseries_external_ids_with_extra_cdf_time_series(
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
        if isinstance(column_names, str) and column_names == "cdfTimeSeries":
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


class InputTimeSeriesMappingCdfTimeSeriesAPI:
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        self._client = client
        self._view_id = view_id

    def __call__(
        self,
        shop_object_type: str | list[str] | None = None,
        shop_object_type_prefix: str | None = None,
        shop_object_name: str | list[str] | None = None,
        shop_object_name_prefix: str | None = None,
        shop_attribute_name: str | list[str] | None = None,
        shop_attribute_name_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InputTimeSeriesMappingCdfTimeSeriesQuery:
        filter_ = _create_filter(
            self._view_id,
            shop_object_type,
            shop_object_type_prefix,
            shop_object_name,
            shop_object_name_prefix,
            shop_attribute_name,
            shop_attribute_name_prefix,
            retrieve,
            retrieve_prefix,
            aggregation,
            aggregation_prefix,
            external_id_prefix,
            filter,
        )

        return InputTimeSeriesMappingCdfTimeSeriesQuery(
            client=self._client,
            view_id=self._view_id,
            timeseries_limit=limit,
            filter=filter_,
        )

    def list(
        self,
        shop_object_type: str | list[str] | None = None,
        shop_object_type_prefix: str | None = None,
        shop_object_name: str | list[str] | None = None,
        shop_object_name_prefix: str | None = None,
        shop_attribute_name: str | list[str] | None = None,
        shop_attribute_name_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TimeSeriesList:
        filter_ = _create_filter(
            self._view_id,
            shop_object_type,
            shop_object_type_prefix,
            shop_object_name,
            shop_object_name_prefix,
            shop_attribute_name,
            shop_attribute_name_prefix,
            retrieve,
            retrieve_prefix,
            aggregation,
            aggregation_prefix,
            external_id_prefix,
            filter,
        )
        external_ids = _retrieve_timeseries_external_ids_with_extra_cdf_time_series(
            self._client, self._view_id, filter_, limit
        )
        if external_ids:
            return self._client.time_series.retrieve_multiple(external_ids=list(external_ids))
        else:
            return TimeSeriesList([])


def _retrieve_timeseries_external_ids_with_extra_cdf_time_series(
    client: CogniteClient,
    view_id: dm.ViewId,
    filter_: dm.Filter | None,
    limit: int,
    extra_properties: ColumnNames | list[ColumnNames] = "cdfTimeSeries",
) -> dict[str, list[str]]:
    properties = ["cdfTimeSeries"]
    if extra_properties == "cdfTimeSeries":
        ...
    elif isinstance(extra_properties, str) and extra_properties != "cdfTimeSeries":
        properties.append(extra_properties)
    elif isinstance(extra_properties, list):
        properties.extend([prop for prop in extra_properties if prop != "cdfTimeSeries"])
    else:
        raise ValueError(f"Invalid value for extra_properties: {extra_properties}")

    if isinstance(extra_properties, str):
        extra_list = [extra_properties]
    else:
        extra_list = extra_properties
    has_data = dm.filters.HasData([dm.ContainerId("power-ops", "InputTimeSeriesMapping")], [view_id])
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
            node.properties[view_id]["cdfTimeSeries"]: [node.properties[view_id].get(prop, "") for prop in extra_list]
            for node in result.data["nodes"].data
        }
        total_retrieved += len(batch_external_ids)
        external_ids.update(batch_external_ids)
        cursor = result.cursors["nodes"]
        if total_retrieved >= limit or cursor is None:
            break
    return external_ids


class InputTimeSeriesMappingTransformationsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="power-ops") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "InputTimeSeriesMapping.transformations"},
        )
        if isinstance(external_id, str):
            is_input_time_series_mapping = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_input_time_series_mapping)
            )

        else:
            is_input_time_series_mappings = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_input_time_series_mappings)
            )

    def list(
        self, input_time_series_mapping_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="power-ops"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "InputTimeSeriesMapping.transformations"},
        )
        filters.append(is_edge_type)
        if input_time_series_mapping_id:
            input_time_series_mapping_ids = (
                [input_time_series_mapping_id]
                if isinstance(input_time_series_mapping_id, str)
                else input_time_series_mapping_id
            )
            is_input_time_series_mappings = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in input_time_series_mapping_ids],
            )
            filters.append(is_input_time_series_mappings)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class InputTimeSeriesMappingAPI(
    TypeAPI[InputTimeSeriesMapping, InputTimeSeriesMappingApply, InputTimeSeriesMappingList]
):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=InputTimeSeriesMapping,
            class_apply_type=InputTimeSeriesMappingApply,
            class_list=InputTimeSeriesMappingList,
        )
        self._view_id = view_id
        self.transformations = InputTimeSeriesMappingTransformationsAPI(client)
        self.cdf_time_series = InputTimeSeriesMappingCdfTimeSeriesAPI(client, view_id)

    def apply(
        self,
        input_time_series_mapping: InputTimeSeriesMappingApply | Sequence[InputTimeSeriesMappingApply],
        replace: bool = False,
    ) -> dm.InstancesApplyResult:
        if isinstance(input_time_series_mapping, InputTimeSeriesMappingApply):
            instances = input_time_series_mapping.to_instances_apply()
        else:
            instances = InputTimeSeriesMappingApplyList(input_time_series_mapping).to_instances_apply()
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
    def retrieve(self, external_id: str) -> InputTimeSeriesMapping:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> InputTimeSeriesMappingList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> InputTimeSeriesMapping | InputTimeSeriesMappingList:
        if isinstance(external_id, str):
            input_time_series_mapping = self._retrieve((self._sources.space, external_id))

            transformation_edges = self.transformations.retrieve(external_id)
            input_time_series_mapping.transformations = [edge.end_node.external_id for edge in transformation_edges]

            return input_time_series_mapping
        else:
            input_time_series_mappings = self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

            transformation_edges = self.transformations.retrieve(external_id)
            self._set_transformations(input_time_series_mappings, transformation_edges)

            return input_time_series_mappings

    def search(
        self,
        query: str,
        properties: InputTimeSeriesMappingTextFields | Sequence[InputTimeSeriesMappingTextFields] | None = None,
        shop_object_type: str | list[str] | None = None,
        shop_object_type_prefix: str | None = None,
        shop_object_name: str | list[str] | None = None,
        shop_object_name_prefix: str | None = None,
        shop_attribute_name: str | list[str] | None = None,
        shop_attribute_name_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InputTimeSeriesMappingList:
        filter_ = _create_filter(
            self._view_id,
            shop_object_type,
            shop_object_type_prefix,
            shop_object_name,
            shop_object_name_prefix,
            shop_attribute_name,
            shop_attribute_name_prefix,
            retrieve,
            retrieve_prefix,
            aggregation,
            aggregation_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(
            self._view_id, query, _INPUTTIMESERIESMAPPING_PROPERTIES_BY_FIELD, properties, filter_, limit
        )

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: InputTimeSeriesMappingFields | Sequence[InputTimeSeriesMappingFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: InputTimeSeriesMappingTextFields | Sequence[InputTimeSeriesMappingTextFields] | None = None,
        shop_object_type: str | list[str] | None = None,
        shop_object_type_prefix: str | None = None,
        shop_object_name: str | list[str] | None = None,
        shop_object_name_prefix: str | None = None,
        shop_attribute_name: str | list[str] | None = None,
        shop_attribute_name_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
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
        property: InputTimeSeriesMappingFields | Sequence[InputTimeSeriesMappingFields] | None = None,
        group_by: InputTimeSeriesMappingFields | Sequence[InputTimeSeriesMappingFields] = None,
        query: str | None = None,
        search_properties: InputTimeSeriesMappingTextFields | Sequence[InputTimeSeriesMappingTextFields] | None = None,
        shop_object_type: str | list[str] | None = None,
        shop_object_type_prefix: str | None = None,
        shop_object_name: str | list[str] | None = None,
        shop_object_name_prefix: str | None = None,
        shop_attribute_name: str | list[str] | None = None,
        shop_attribute_name_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
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
        property: InputTimeSeriesMappingFields | Sequence[InputTimeSeriesMappingFields] | None = None,
        group_by: InputTimeSeriesMappingFields | Sequence[InputTimeSeriesMappingFields] | None = None,
        query: str | None = None,
        search_property: InputTimeSeriesMappingTextFields | Sequence[InputTimeSeriesMappingTextFields] | None = None,
        shop_object_type: str | list[str] | None = None,
        shop_object_type_prefix: str | None = None,
        shop_object_name: str | list[str] | None = None,
        shop_object_name_prefix: str | None = None,
        shop_attribute_name: str | list[str] | None = None,
        shop_attribute_name_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            shop_object_type,
            shop_object_type_prefix,
            shop_object_name,
            shop_object_name_prefix,
            shop_attribute_name,
            shop_attribute_name_prefix,
            retrieve,
            retrieve_prefix,
            aggregation,
            aggregation_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _INPUTTIMESERIESMAPPING_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: InputTimeSeriesMappingFields,
        interval: float,
        query: str | None = None,
        search_property: InputTimeSeriesMappingTextFields | Sequence[InputTimeSeriesMappingTextFields] | None = None,
        shop_object_type: str | list[str] | None = None,
        shop_object_type_prefix: str | None = None,
        shop_object_name: str | list[str] | None = None,
        shop_object_name_prefix: str | None = None,
        shop_attribute_name: str | list[str] | None = None,
        shop_attribute_name_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            shop_object_type,
            shop_object_type_prefix,
            shop_object_name,
            shop_object_name_prefix,
            shop_attribute_name,
            shop_attribute_name_prefix,
            retrieve,
            retrieve_prefix,
            aggregation,
            aggregation_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _INPUTTIMESERIESMAPPING_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        shop_object_type: str | list[str] | None = None,
        shop_object_type_prefix: str | None = None,
        shop_object_name: str | list[str] | None = None,
        shop_object_name_prefix: str | None = None,
        shop_attribute_name: str | list[str] | None = None,
        shop_attribute_name_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> InputTimeSeriesMappingList:
        filter_ = _create_filter(
            self._view_id,
            shop_object_type,
            shop_object_type_prefix,
            shop_object_name,
            shop_object_name_prefix,
            shop_attribute_name,
            shop_attribute_name_prefix,
            retrieve,
            retrieve_prefix,
            aggregation,
            aggregation_prefix,
            external_id_prefix,
            filter,
        )

        input_time_series_mappings = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := input_time_series_mappings.as_external_ids()) > IN_FILTER_LIMIT:
                transformation_edges = self.transformations.list(limit=-1)
            else:
                transformation_edges = self.transformations.list(external_ids, limit=-1)
            self._set_transformations(input_time_series_mappings, transformation_edges)

        return input_time_series_mappings

    @staticmethod
    def _set_transformations(
        input_time_series_mappings: Sequence[InputTimeSeriesMapping], transformation_edges: Sequence[dm.Edge]
    ):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in transformation_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for input_time_series_mapping in input_time_series_mappings:
            node_id = input_time_series_mapping.id_tuple()
            if node_id in edges_by_start_node:
                input_time_series_mapping.transformations = [
                    edge.end_node.external_id for edge in edges_by_start_node[node_id]
                ]


def _create_filter(
    view_id: dm.ViewId,
    shop_object_type: str | list[str] | None = None,
    shop_object_type_prefix: str | None = None,
    shop_object_name: str | list[str] | None = None,
    shop_object_name_prefix: str | None = None,
    shop_attribute_name: str | list[str] | None = None,
    shop_attribute_name_prefix: str | None = None,
    retrieve: str | list[str] | None = None,
    retrieve_prefix: str | None = None,
    aggregation: str | list[str] | None = None,
    aggregation_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if shop_object_type and isinstance(shop_object_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopObjectType"), value=shop_object_type))
    if shop_object_type and isinstance(shop_object_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("shopObjectType"), values=shop_object_type))
    if shop_object_type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("shopObjectType"), value=shop_object_type_prefix))
    if shop_object_name and isinstance(shop_object_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopObjectName"), value=shop_object_name))
    if shop_object_name and isinstance(shop_object_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("shopObjectName"), values=shop_object_name))
    if shop_object_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("shopObjectName"), value=shop_object_name_prefix))
    if shop_attribute_name and isinstance(shop_attribute_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopAttributeName"), value=shop_attribute_name))
    if shop_attribute_name and isinstance(shop_attribute_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("shopAttributeName"), values=shop_attribute_name))
    if shop_attribute_name_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("shopAttributeName"), value=shop_attribute_name_prefix)
        )
    if retrieve and isinstance(retrieve, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("retrieve"), value=retrieve))
    if retrieve and isinstance(retrieve, list):
        filters.append(dm.filters.In(view_id.as_property_ref("retrieve"), values=retrieve))
    if retrieve_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("retrieve"), value=retrieve_prefix))
    if aggregation and isinstance(aggregation, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("aggregation"), value=aggregation))
    if aggregation and isinstance(aggregation, list):
        filters.append(dm.filters.In(view_id.as_property_ref("aggregation"), values=aggregation))
    if aggregation_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("aggregation"), value=aggregation_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
