from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import Literal

import pandas as pd
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import Datapoints, DatapointsArrayList, DatapointsList, TimeSeriesList
from cognite.client.data_classes.datapoints import Aggregate
from cognite.powerops.client._generated.v1.data_classes._partial_bid_matrix_information import (
    _create_partial_bid_matrix_information_filter,
)
from ._core import DEFAULT_LIMIT_READ, INSTANCE_QUERY_LIMIT

ColumnNames = Literal["state", "bidMatrix", "linkedTimeSeries", "resourceCost"]


class PartialBidMatrixInformationLinkedTimeSeriesQuery:
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
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
    ) -> DatapointsList:
        """`Retrieve datapoints for the `partial_bid_matrix_information.linked_time_series` timeseries.

        **Performance guide**:
            In order to retrieve millions of datapoints as efficiently as possible, here are a few guidelines:

            1. For the best speed, and significantly lower memory usage, consider using ``retrieve_arrays(...)`` which uses ``numpy.ndarrays`` for data storage.
            2. Only unlimited queries with (``limit=None``) are fetched in parallel, so specifying a large finite ``limit`` like 1 million, comes with severe performance penalty as data is fetched serially.
            3. Try to avoid specifying `start` and `end` to be very far from the actual data: If you had data from 2000 to 2015, don't set start=0 (1970).

        Args:
            start: Inclusive start. Default: 1970-01-01 UTC.
            end: Exclusive end. Default: "now"
            aggregates: Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity The granularity to fetch aggregates at. e.g. '15s', '2h', '10d'. Default: None.
            target_unit: The unit_external_id of the data points returned. If the time series does not have an unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system: The unit system of the data points returned. Cannot be used with target_unit.
            limit (int | None): Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points (bool): Whether to include outside points. Not allowed when fetching aggregates. Default: False

        Returns:
            A ``DatapointsList`` with the requested datapoints.

        Examples:

            In this example,
            we are using the time-ago format to get raw data for the 'my_linked_time_series' from 2 weeks ago up until now::

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> partial_bid_matrix_information_datapoints = client.partial_bid_matrix_information.linked_time_series(external_id="my_linked_time_series").retrieve(start="2w-ago")
        """
        external_ids = self._retrieve_timeseries_external_ids_with_extra()
        if external_ids:
            return self._client.time_series.data.retrieve(
                external_id=list(external_ids),
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                target_unit=target_unit,
                target_unit_system=target_unit_system,
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
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
    ) -> DatapointsArrayList:
        """`Retrieve numpy arrays for the `partial_bid_matrix_information.linked_time_series` timeseries.

        **Performance guide**:
            In order to retrieve millions of datapoints as efficiently as possible, here are a few guidelines:

            1. For the best speed, and significantly lower memory usage, consider using ``retrieve_arrays(...)`` which uses ``numpy.ndarrays`` for data storage.
            2. Only unlimited queries with (``limit=None``) are fetched in parallel, so specifying a large finite ``limit`` like 1 million, comes with severe performance penalty as data is fetched serially.
            3. Try to avoid specifying `start` and `end` to be very far from the actual data: If you had data from 2000 to 2015, don't set start=0 (1970).

        Args:
            start: Inclusive start. Default: 1970-01-01 UTC.
            end: Exclusive end. Default: "now"
            aggregates: Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity The granularity to fetch aggregates at. e.g. '15s', '2h', '10d'. Default: None.
            target_unit: The unit_external_id of the data points returned. If the time series does not have an unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system: The unit system of the data points returned. Cannot be used with target_unit.
            limit (int | None): Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points (bool): Whether to include outside points. Not allowed when fetching aggregates. Default: False

        Returns:
            A ``DatapointsArrayList`` with the requested datapoints.

        Examples:

            In this example,
            we are using the time-ago format to get raw data for the 'my_linked_time_series' from 2 weeks ago up until now::

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> partial_bid_matrix_information_datapoints = client.partial_bid_matrix_information.linked_time_series(external_id="my_linked_time_series").retrieve_array(start="2w-ago")
        """
        external_ids = self._retrieve_timeseries_external_ids_with_extra()
        if external_ids:
            return self._client.time_series.data.retrieve_arrays(
                external_id=list(external_ids),
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                target_unit=target_unit,
                target_unit_system=target_unit_system,
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
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        column_names: ColumnNames | list[ColumnNames] = "linkedTimeSeries",
    ) -> pd.DataFrame:
        """`Retrieve DataFrames for the `partial_bid_matrix_information.linked_time_series` timeseries.

        **Performance guide**:
            In order to retrieve millions of datapoints as efficiently as possible, here are a few guidelines:

            1. For the best speed, and significantly lower memory usage, consider using ``retrieve_arrays(...)`` which uses ``numpy.ndarrays`` for data storage.
            2. Only unlimited queries with (``limit=None``) are fetched in parallel, so specifying a large finite ``limit`` like 1 million, comes with severe performance penalty as data is fetched serially.
            3. Try to avoid specifying `start` and `end` to be very far from the actual data: If you had data from 2000 to 2015, don't set start=0 (1970).

        Args:
            start: Inclusive start. Default: 1970-01-01 UTC.
            end: Exclusive end. Default: "now"
            aggregates: Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity The granularity to fetch aggregates at. e.g. '15s', '2h', '10d'. Default: None.
            target_unit: The unit_external_id of the data points returned. If the time series does not have an unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system: The unit system of the data points returned. Cannot be used with target_unit.
            limit: Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points: Whether to include outside points. Not allowed when fetching aggregates. Default: False
            uniform_index: If only querying aggregates AND a single granularity is used, AND no limit is used, specifying `uniform_index=True` will return a dataframe with an equidistant datetime index from the earliest `start` to the latest `end` (missing values will be NaNs). If these requirements are not met, a ValueError is raised. Default: False
            include_aggregate_name: Include 'aggregate' in the column name, e.g. `my-ts|average`. Ignored for raw time series. Default: True
            include_granularity_name: Include 'granularity' in the column name, e.g. `my-ts|12h`. Added after 'aggregate' when present. Ignored for raw time series. Default: False
            column_names: Which property to use for column names. Defauts to linkedTimeSeries


        Returns:
            A ``DataFrame`` with the requested datapoints.

        Examples:

            In this example,
            we are using the time-ago format to get raw data for the 'my_linked_time_series' from 2 weeks ago up until now::

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> partial_bid_matrix_information_datapoints = client.partial_bid_matrix_information.linked_time_series(external_id="my_linked_time_series").retrieve_dataframe(start="2w-ago")
        """
        external_ids = self._retrieve_timeseries_external_ids_with_extra(column_names)
        if external_ids:
            df = self._client.time_series.data.retrieve_dataframe(
                external_id=list(external_ids),
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                target_unit=target_unit,
                target_unit_system=target_unit_system,
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
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        column_names: ColumnNames | list[ColumnNames] = "linkedTimeSeries",
    ) -> pd.DataFrame:
        """Retrieve DataFrames for the `partial_bid_matrix_information.linked_time_series` timeseries in Timezone.

        **Performance guide**:
            In order to retrieve millions of datapoints as efficiently as possible, here are a few guidelines:

            1. For the best speed, and significantly lower memory usage, consider using ``retrieve_arrays(...)`` which uses ``numpy.ndarrays`` for data storage.
            2. Only unlimited queries with (``limit=None``) are fetched in parallel, so specifying a large finite ``limit`` like 1 million, comes with severe performance penalty as data is fetched serially.
            3. Try to avoid specifying `start` and `end` to be very far from the actual data: If you had data from 2000 to 2015, don't set start=0 (1970).

        Args:
            start: Inclusive start.
            end: Exclusive end
            aggregates: Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity The granularity to fetch aggregates at. e.g. '15s', '2h', '10d'. Default: None.
            target_unit: The unit_external_id of the data points returned. If the time series does not have an unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system: The unit system of the data points returned. Cannot be used with target_unit.
            limit: Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points: Whether to include outside points. Not allowed when fetching aggregates. Default: False
            uniform_index: If only querying aggregates AND a single granularity is used, AND no limit is used, specifying `uniform_index=True` will return a dataframe with an equidistant datetime index from the earliest `start` to the latest `end` (missing values will be NaNs). If these requirements are not met, a ValueError is raised. Default: False
            include_aggregate_name: Include 'aggregate' in the column name, e.g. `my-ts|average`. Ignored for raw time series. Default: True
            include_granularity_name: Include 'granularity' in the column name, e.g. `my-ts|12h`. Added after 'aggregate' when present. Ignored for raw time series. Default: False
            column_names: Which property to use for column names. Defauts to linkedTimeSeries


        Returns:
            A ``DataFrame`` with the requested datapoints.

        Examples:

            In this example,
            get weekly aggregates for the 'my_linked_time_series' for the first month of 2023 in Oslo time:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from datetime import datetime, timezone
                >>> client = PowerOpsModelsV1Client()
                >>> partial_bid_matrix_information_datapoints = client.partial_bid_matrix_information.linked_time_series(
                ...     external_id="my_linked_time_series").retrieve_dataframe_in_timezone(
                ...         datetime(2023, 1, 1, tzinfo=ZoneInfo("Europe/Oslo")),
                ...         datetime(2023, 1, 2, tzinfo=ZoneInfo("Europe/Oslo")),
                ...         aggregates="average",
                ...         granularity="1week",
                ...     )
        """
        external_ids = self._retrieve_timeseries_external_ids_with_extra(column_names)
        if external_ids:
            df = self._client.time_series.data.retrieve_dataframe_in_tz(
                external_id=list(external_ids),
                start=start,
                end=end,
                aggregates=aggregates,
                granularity=granularity,
                target_unit=target_unit,
                target_unit_system=target_unit_system,
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

    def _retrieve_timeseries_external_ids_with_extra(
        self, extra_properties: ColumnNames | list[ColumnNames] = "linkedTimeSeries"
    ) -> dict[str, list[str]]:
        return _retrieve_timeseries_external_ids_with_extra_linked_time_series(
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
        if isinstance(column_names, str) and column_names == "linkedTimeSeries":
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


class PartialBidMatrixInformationLinkedTimeSeriesAPI:
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        self._client = client
        self._view_id = view_id

    def __call__(
        self,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_resource_cost: float | None = None,
        max_resource_cost: float | None = None,
        partial_bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PartialBidMatrixInformationLinkedTimeSeriesQuery:
        """Query timeseries `partial_bid_matrix_information.linked_time_series`

        Args:
            state: The state to filter on.
            state_prefix: The prefix of the state to filter on.
            power_asset: The power asset to filter on.
            min_resource_cost: The minimum value of the resource cost to filter on.
            max_resource_cost: The maximum value of the resource cost to filter on.
            partial_bid_configuration: The partial bid configuration to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial bid matrix information to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query object that can be used to retrieve datapoins for the partial_bid_matrix_information.linked_time_series timeseries
            selected in this method.

        Examples:

            Retrieve all data for 5 partial_bid_matrix_information.linked_time_series timeseries:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> partial_bid_matrix_information_list = client.partial_bid_matrix_information.linked_time_series(limit=5).retrieve()

        """
        filter_ = _create_partial_bid_matrix_information_filter(
            self._view_id,
            state,
            state_prefix,
            power_asset,
            min_resource_cost,
            max_resource_cost,
            partial_bid_configuration,
            external_id_prefix,
            space,
            filter,
        )

        return PartialBidMatrixInformationLinkedTimeSeriesQuery(
            client=self._client,
            view_id=self._view_id,
            timeseries_limit=limit,
            filter=filter_,
        )

    def list(
        self,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_resource_cost: float | None = None,
        max_resource_cost: float | None = None,
        partial_bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TimeSeriesList:
        """List timeseries `partial_bid_matrix_information.linked_time_series`

        Args:
            state: The state to filter on.
            state_prefix: The prefix of the state to filter on.
            power_asset: The power asset to filter on.
            min_resource_cost: The minimum value of the resource cost to filter on.
            max_resource_cost: The maximum value of the resource cost to filter on.
            partial_bid_configuration: The partial bid configuration to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial bid matrix information to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of Timeseries partial_bid_matrix_information.linked_time_series.

        Examples:

            List partial_bid_matrix_information.linked_time_series and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> partial_bid_matrix_information_list = client.partial_bid_matrix_information.linked_time_series.list(limit=5)

        """
        filter_ = _create_partial_bid_matrix_information_filter(
            self._view_id,
            state,
            state_prefix,
            power_asset,
            min_resource_cost,
            max_resource_cost,
            partial_bid_configuration,
            external_id_prefix,
            space,
            filter,
        )
        external_ids = _retrieve_timeseries_external_ids_with_extra_linked_time_series(
            self._client, self._view_id, filter_, limit
        )
        if external_ids:
            return self._client.time_series.retrieve_multiple(external_ids=list(external_ids))
        else:
            return TimeSeriesList([])


def _retrieve_timeseries_external_ids_with_extra_linked_time_series(
    client: CogniteClient,
    view_id: dm.ViewId,
    filter_: dm.Filter | None,
    limit: int,
    extra_properties: ColumnNames | list[ColumnNames] = "linkedTimeSeries",
) -> dict[str, list[str]]:
    limit = float("inf") if limit is None or limit == -1 else limit
    properties = ["linkedTimeSeries"]
    if extra_properties == "linkedTimeSeries":
        ...
    elif isinstance(extra_properties, str) and extra_properties != "linkedTimeSeries":
        properties.append(extra_properties)
    elif isinstance(extra_properties, list):
        properties.extend([prop for prop in extra_properties if prop != "linkedTimeSeries"])
    else:
        raise ValueError(f"Invalid value for extra_properties: {extra_properties}")

    if isinstance(extra_properties, str):
        extra_list = [extra_properties]
    else:
        extra_list = extra_properties
    has_data = dm.filters.HasData(views=[view_id])
    has_property = dm.filters.Exists(property=view_id.as_property_ref("linkedTimeSeries"))
    filter_ = dm.filters.And(filter_, has_data, has_property) if filter_ else dm.filters.And(has_data, has_property)

    cursor = None
    external_ids: dict[str, list[str]] = {}
    total_retrieved = 0
    while True:
        query_limit = max(min(INSTANCE_QUERY_LIMIT, limit - total_retrieved), 0)
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
            node.properties[view_id]["linkedTimeSeries"]: [
                node.properties[view_id].get(prop, "") for prop in extra_list
            ]
            for node in result.data["nodes"].data
        }
        total_retrieved += len(batch_external_ids)
        external_ids.update(batch_external_ids)
        cursor = result.cursors["nodes"]
        if total_retrieved >= limit or cursor is None:
            break
    return external_ids
