from __future__ import annotations

import datetime
import warnings
from collections.abc import Sequence
from typing import Literal

import pandas as pd
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import Datapoints, DatapointsArrayList, DatapointsList, TimeSeriesList
from cognite.client.data_classes.datapoints import Aggregate
from cognite.client.utils._time import ZoneInfo

from cognite.powerops.client._generated.v1._api._core import DEFAULT_LIMIT_READ
from cognite.powerops.client._generated.v1.data_classes._generator import _create_generator_filter
from cognite.powerops.client._generated.v1.data_classes._core import QueryBuilder, QueryStep

ColumnNames = Literal["name", "displayName", "ordering", "assetType", "productionMin", "productionMax", "penstockNumber", "startStopCost", "startStopCostTimeSeries", "availabilityTimeSeries"]


class GeneratorStartStopCostTimeSeriesQuery:
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
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
    ) -> DatapointsList:
        """`Retrieve datapoints for the `generator.start_stop_cost_time_series` timeseries.

        **Performance guide**:
            In order to retrieve millions of datapoints as efficiently as possible, here are a few guidelines:

            1. For the best speed, and significantly lower memory usage, consider using
               ``retrieve_arrays(...)`` which uses ``numpy.ndarrays`` for data storage.
            2. Only unlimited queries with (``limit=None``) are fetched in parallel, so specifying a large
               finite ``limit`` like 1 million, comes with severe performance penalty as data is fetched serially.
            3. Try to avoid specifying `start` and `end` to be very far from the actual data: If you had data
               from 2000 to 2015, don't set start=0 (1970).

        Args:
            start: Inclusive start. Default: 1970-01-01 UTC.
            end: Exclusive end. Default: "now"
            aggregates: Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity The granularity to fetch aggregates at. e.g. '15s', '2h', '10d'. Default: None.
            timezone (str | datetime.timezone | ZoneInfo | None): For raw datapoints, which timezone
                to use when displaying (will not affect what is retrieved). For aggregates, which timezone
                to align to for granularity 'hour' and longer. Align to the start of the hour, -day or -month.
                For timezones of type Region/Location, like 'Europe/Oslo', pass a string or ``ZoneInfo`` instance.
                The aggregate duration will then vary, typically due to daylight saving time. You can also use a
                fixed offset from UTC by passing a string like '+04:00', 'UTC-7' or 'UTC-02:30' or an instance of
                ``datetime.timezone``. Note: Historical timezones with second offset are not supported,
                and timezones with minute offsets (e.g. UTC+05:30 or Asia/Kolkata) may take longer to execute.
            target_unit: The unit_external_id of the data points returned. If the time series does not have an
                unit_external_id that can be converted to the target_unit, an error will be returned.
                Cannot be used with target_unit_system.
            target_unit_system: The unit system of the data points returned. Cannot be used with target_unit.
            limit: Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points: Whether to include outside points. Not allowed when fetching aggregates.
                Default: False
            include_status (bool): Also return the status code, an integer, for each datapoint in the response.
                Only relevant for raw datapoint queries, not aggregates.
            ignore_bad_datapoints (bool): Treat datapoints with a bad status code as if they do not exist.
                If set to false, raw queries will include bad datapoints in the response, and aggregates
                will in general omit the time period between a bad datapoint and the next good datapoint.
                Also, the period between a bad datapoint and the previous good datapoint will be considered
                constant. Default: True.
            treat_uncertain_as_bad (bool): Treat datapoints with uncertain status codes as bad. If false,
                treat datapoints with uncertain status codes as good. Used for both raw queries and aggregates.
                Default: True.

        Returns:
            A ``DatapointsList`` with the requested datapoints.

        Examples:

            In this example,
            we are using the time-ago format to get raw data for the
            'my_start_stop_cost_time_series' from 2 weeks ago up until now::

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> generator_datapoints = client.generator.start_stop_cost_time_series(
                ...         external_id="my_start_stop_cost_time_series"
                ...     ).retrieve(start="2w-ago")
        """
        external_ids = self._retrieve_timeseries_external_ids_with_extra()
        if external_ids:
            # Missing overload in SDK
            return self._client.time_series.data.retrieve(  # type: ignore[return-value]
                external_id=list(external_ids),
                start=start,
                end=end,
                aggregates=aggregates,  # type: ignore[arg-type]
                granularity=granularity,
                timezone=timezone,
                target_unit=target_unit,
                target_unit_system=target_unit_system,
                limit=limit,
                include_outside_points=include_outside_points,
                include_status=include_status,
                ignore_bad_datapoints=ignore_bad_datapoints,
                treat_uncertain_as_bad=treat_uncertain_as_bad,
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
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
    ) -> DatapointsArrayList:
        """`Retrieve numpy arrays for the `generator.start_stop_cost_time_series` timeseries.

        **Performance guide**:
            In order to retrieve millions of datapoints as efficiently as possible, here are a few guidelines:

            1. For the best speed, and significantly lower memory usage, consider using
               ``retrieve_arrays(...)`` which uses ``numpy.ndarrays`` for data storage.
            2. Only unlimited queries with (``limit=None``) are fetched in parallel, so specifying a large
               finite ``limit`` like 1 million, comes with severe performance penalty as data is fetched serially.
            3. Try to avoid specifying `start` and `end` to be very far from the actual data: If you had
               data from 2000 to 2015, don't set start=0 (1970).

        Args:
            start: Inclusive start. Default: 1970-01-01 UTC.
            end: Exclusive end. Default: "now"
            aggregates: Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity The granularity to fetch aggregates at. e.g. '15s', '2h', '10d'. Default: None.
            timezone (str | datetime.timezone | ZoneInfo | None): For raw datapoints, which timezone to use when
                displaying (will not affect what is retrieved). For aggregates, which timezone to align to for
                granularity 'hour' and longer. Align to the start of the hour, -day or -month. For timezones of
                type Region/Location, like 'Europe/Oslo', pass a string or ``ZoneInfo`` instance. The aggregate
                duration will then vary, typically due to daylight saving time. You can also use a fixed offset
                from UTC by passing a string like '+04:00', 'UTC-7' or 'UTC-02:30' or an instance of
                ``datetime.timezone``. Note: Historical timezones with second offset are not supported, and timezones
                with minute offsets (e.g. UTC+05:30 or Asia/Kolkata) may take longer to execute.
            target_unit: The unit_external_id of the data points returned. If the time series does not have an
                unit_external_id that can be converted to the target_unit, an error will be returned.
                Cannot be used with target_unit_system.
            target_unit_system: The unit system of the data points returned. Cannot be used with target_unit.
            limit: Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points: Whether to include outside points. Not allowed when fetching aggregates.
                Default: False
            include_status (bool): Also return the status code, an integer, for each datapoint in the response.
                Only relevant for raw datapoint queries, not aggregates.
            ignore_bad_datapoints (bool): Treat datapoints with a bad status code as if they do not exist.
                If set to false, raw queries will include bad datapoints in the response, and aggregates will
                in general omit the time period between a bad datapoint and the next good datapoint. Also, the
                period between a bad datapoint and the previous good datapoint will be considered constant.
                Default: True.
            treat_uncertain_as_bad (bool): Treat datapoints with uncertain status codes as bad. If false,
                treat datapoints with uncertain status codes as good. Used for both raw queries and aggregates.
                Default: True.

        Returns:
            A ``DatapointsArrayList`` with the requested datapoints.

        Examples:

            In this example,
            we are using the time-ago format to get raw data for the 'my_start_stop_cost_time_series'
            from 2 weeks ago up until now:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> generator_datapoints = client.generator.start_stop_cost_time_series(
                ...     external_id="my_start_stop_cost_time_series"
                ... ).retrieve_array(start="2w-ago")
        """
        external_ids = self._retrieve_timeseries_external_ids_with_extra()
        if external_ids:
            # Missing overload in SDK
            return self._client.time_series.data.retrieve_arrays(  # type: ignore[return-value]
                external_id=list(external_ids),
                start=start,
                end=end,
                aggregates=aggregates,  # type: ignore[arg-type]
                granularity=granularity,
                timezone=timezone,
                target_unit=target_unit,
                target_unit_system=target_unit_system,
                limit=limit,
                include_outside_points=include_outside_points,
                include_status=include_status,
                ignore_bad_datapoints=ignore_bad_datapoints,
                treat_uncertain_as_bad=treat_uncertain_as_bad,
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
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        include_outside_points: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
        column_names: ColumnNames | list[ColumnNames] = "startStopCostTimeSeries",
    ) -> pd.DataFrame:
        """`Retrieve DataFrames for the `generator.start_stop_cost_time_series` timeseries.

        **Performance guide**:
            In order to retrieve millions of datapoints as efficiently as possible, here are a few guidelines:

            1. For the best speed, and significantly lower memory usage, consider using
               ``retrieve_arrays(...)`` which uses ``numpy.ndarrays`` for data storage.
            2. Only unlimited queries with (``limit=None``) are fetched in parallel, so specifying a
               large finite ``limit`` like 1 million, comes with severe performance penalty as data is fetched serially.
            3. Try to avoid specifying `start` and `end` to be very far from the actual data: If you had data
               from 2000 to 2015, don't set start=0 (1970).

        Args:
            start: Inclusive start. Default: 1970-01-01 UTC.
            end: Exclusive end. Default: "now"
            aggregates: Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity The granularity to fetch aggregates at. e.g. '15s', '2h', '10d'. Default: None.
            timezone (str | datetime.timezone | ZoneInfo | None): For raw datapoints, which timezone to use when
                displaying (will not affect what is retrieved). For aggregates, which timezone to align to for
                granularity 'hour' and longer. Align to the start of the hour, -day or -month. For timezones of
                type Region/Location, like 'Europe/Oslo', pass a string or ``ZoneInfo`` instance. The aggregate
                duration will then vary, typically due to daylight saving time. You can also use a fixed offset
                from UTC by passing a string like '+04:00', 'UTC-7' or 'UTC-02:30' or an instance of
                ``datetime.timezone``. Note: Historical timezones with second offset are not supported,
                and timezones with minute offsets (e.g. UTC+05:30 or Asia/Kolkata) may take longer to execute.
            target_unit: The unit_external_id of the data points returned. If the time series does not have an
                unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used
                with target_unit_system.
            target_unit_system: The unit system of the data points returned. Cannot be used with target_unit.
            limit: Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points: Whether to include outside points. Not allowed when fetching aggregates.
                Default: False
            include_status (bool): Also return the status code, an integer, for each datapoint in the response.
                Only relevant for raw datapoint queries, not aggregates.
            ignore_bad_datapoints (bool): Treat datapoints with a bad status code as if they do not exist.
                If set to false, raw queries will include bad datapoints in the response, and aggregates will
                in general omit the time period between a bad datapoint and the next good datapoint. Also,
                the period between a bad datapoint and the previous good datapoint will be considered
                constant. Default: True.
            treat_uncertain_as_bad (bool): Treat datapoints with uncertain status codes as bad. If false,
                treat datapoints with uncertain status codes as good. Used for both raw queries and aggregates.
                Default: True.
            uniform_index: If only querying aggregates AND a single granularity is used, AND no limit is used,
                specifying `uniform_index=True` will return a dataframe with an equidistant datetime index from
                the earliest `start` to the latest `end` (missing values will be NaNs). If these requirements
                are not met, a ValueError is raised. Default: False
            include_aggregate_name: Include 'aggregate' in the column name, e.g. `my-ts|average`.
                Ignored for raw time series. Default: True
            include_granularity_name: Include 'granularity' in the column name, e.g. `my-ts|12h`. Added after
                'aggregate' when present. Ignored for raw time series. Default: False
            column_names: Which property to use for column names. Defauts to startStopCostTimeSeries


        Returns:
            A ``DataFrame`` with the requested datapoints.

        Examples:

            In this example,
            we are using the time-ago format to get raw data for the 'my_start_stop_cost_time_series'
            from 2 weeks ago up until now::

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> generator_datapoints = client.generator.start_stop_cost_time_series(
                ...     external_id="my_start_stop_cost_time_series"
                ... ).retrieve_dataframe(start="2w-ago")
        """
        external_ids = self._retrieve_timeseries_external_ids_with_extra(column_names)
        if external_ids:
            df = self._client.time_series.data.retrieve_dataframe(
                external_id=list(external_ids),
                start=start,
                end=end,
                aggregates=aggregates,  # type: ignore[arg-type]
                granularity=granularity,
                timezone=timezone,
                target_unit=target_unit,
                target_unit_system=target_unit_system,
                limit=limit,
                include_outside_points=include_outside_points,
                include_status=include_status,
                ignore_bad_datapoints=ignore_bad_datapoints,
                treat_uncertain_as_bad=treat_uncertain_as_bad,
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
        column_names: ColumnNames | list[ColumnNames] = "startStopCostTimeSeries",
    ) -> pd.DataFrame:
        """Retrieve DataFrames for the `generator.start_stop_cost_time_series` timeseries in Timezone.

        **Performance guide**:
            In order to retrieve millions of datapoints as efficiently as possible, here are a few guidelines:

            1. For the best speed, and significantly lower memory usage, consider using
               ``retrieve_arrays(...)`` which uses ``numpy.ndarrays`` for data storage.
            2. Only unlimited queries with (``limit=None``) are fetched in parallel, so specifying a large finite
               ``limit`` like 1 million, comes with severe performance penalty as data is fetched serially.
            3. Try to avoid specifying `start` and `end` to be very far from the actual data: If you had data
               from 2000 to 2015, don't set start=0 (1970).

        Args:
            start: Inclusive start.
            end: Exclusive end
            aggregates: Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity The granularity to fetch aggregates at. e.g. '15s', '2h', '10d'. Default: None.
            target_unit: The unit_external_id of the data points returned. If the time series does not have an
                unit_external_id that can be converted to the target_unit, an error will be returned.
                Cannot be used with target_unit_system.
            target_unit_system: The unit system of the data points returned. Cannot be used with target_unit.
            limit: Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points: Whether to include outside points. Not allowed when fetching aggregates.
                Default: False
            uniform_index: If only querying aggregates AND a single granularity is used, AND no limit is used,
                specifying `uniform_index=True` will return a dataframe with an equidistant datetime index from
                the earliest `start` to the latest `end` (missing values will be NaNs). If these requirements
                are not met, a ValueError is raised. Default: False
            include_aggregate_name: Include 'aggregate' in the column name, e.g. `my-ts|average`. Ignored for
                raw time series. Default: True
            include_granularity_name: Include 'granularity' in the column name, e.g. `my-ts|12h`. Added after
                'aggregate' when present. Ignored for raw time series. Default: False
            column_names: Which property to use for column names. Defauts to startStopCostTimeSeries


        Returns:
            A ``DataFrame`` with the requested datapoints.

        Examples:

            In this example,
            get weekly aggregates for the 'my_start_stop_cost_time_series' for the
            first month of 2023 in Oslo time:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from datetime import datetime, timezone
                >>> client = PowerOpsModelsV1Client()
                >>> generator_datapoints = client.generator.start_stop_cost_time_series(
                ...     external_id="my_start_stop_cost_time_series").retrieve_dataframe_in_timezone(
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
                aggregates=aggregates,  # type: ignore[arg-type]
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
        self, extra_properties: ColumnNames | list[ColumnNames] = "startStopCostTimeSeries"
    ) -> dict[str, list[str]]:
        return _retrieve_timeseries_external_ids_with_extra_start_stop_cost_time_series(
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
        if isinstance(column_names, str) and column_names == "startStopCostTimeSeries":
            return df
        splits = sum(included for included in [include_aggregate_name, include_granularity_name])
        if splits == 0:
            df.columns = ["-".join(external_ids[external_id]) for external_id in df.columns]  # type: ignore[assignment]
        else:
            column_parts = (col.rsplit("|", maxsplit=splits) for col in df.columns)
            df.columns = [  # type: ignore[assignment]
                "-".join(external_ids[external_id]) + "|" + "|".join(parts) for external_id, *parts in column_parts
            ]
        return df


class GeneratorStartStopCostTimeSeriesAPI:
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        self._client = client
        self._view_id = view_id

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        min_production_min: float | None = None,
        max_production_min: float | None = None,
        min_production_max: float | None = None,
        max_production_max: float | None = None,
        min_penstock_number: int | None = None,
        max_penstock_number: int | None = None,
        min_start_stop_cost: float | None = None,
        max_start_stop_cost: float | None = None,
        generator_efficiency_curve: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> GeneratorStartStopCostTimeSeriesQuery:
        """Query timeseries `generator.start_stop_cost_time_series`

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            min_production_min: The minimum value of the production min to filter on.
            max_production_min: The maximum value of the production min to filter on.
            min_production_max: The minimum value of the production max to filter on.
            max_production_max: The maximum value of the production max to filter on.
            min_penstock_number: The minimum value of the penstock number to filter on.
            max_penstock_number: The maximum value of the penstock number to filter on.
            min_start_stop_cost: The minimum value of the start stop cost to filter on.
            max_start_stop_cost: The maximum value of the start stop cost to filter on.
            generator_efficiency_curve: The generator efficiency curve to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generators to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own
                filtering which will be ANDed with the filter above.

        Returns:
            A query object that can be used to retrieve datapoins for
            the generator.start_stop_cost_time_series timeseries
            selected in this method.

        Examples:

            Retrieve all data for 5 generator.start_stop_cost_time_series timeseries:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> generators = client.generator.start_stop_cost_time_series(
                ...     limit=5
                ... ).retrieve()

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. "
            "Use the .select()...data.retrieve_dataframe() method instead.",
            UserWarning,
            stacklevel=2,
        )
        filter_ = _create_generator_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            asset_type,
            asset_type_prefix,
            min_production_min,
            max_production_min,
            min_production_max,
            max_production_max,
            min_penstock_number,
            max_penstock_number,
            min_start_stop_cost,
            max_start_stop_cost,
            generator_efficiency_curve,
            external_id_prefix,
            space,
            filter,
        )

        return GeneratorStartStopCostTimeSeriesQuery(
            client=self._client,
            view_id=self._view_id,
            timeseries_limit=limit,
            filter=filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        min_production_min: float | None = None,
        max_production_min: float | None = None,
        min_production_max: float | None = None,
        max_production_max: float | None = None,
        min_penstock_number: int | None = None,
        max_penstock_number: int | None = None,
        min_start_stop_cost: float | None = None,
        max_start_stop_cost: float | None = None,
        generator_efficiency_curve: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TimeSeriesList:
        """List timeseries `generator.start_stop_cost_time_series`

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            min_production_min: The minimum value of the production min to filter on.
            max_production_min: The maximum value of the production min to filter on.
            min_production_max: The minimum value of the production max to filter on.
            max_production_max: The maximum value of the production max to filter on.
            min_penstock_number: The minimum value of the penstock number to filter on.
            max_penstock_number: The maximum value of the penstock number to filter on.
            min_start_stop_cost: The minimum value of the start stop cost to filter on.
            max_start_stop_cost: The maximum value of the start stop cost to filter on.
            generator_efficiency_curve: The generator efficiency curve to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generators to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own
                filtering which will be ANDed with the filter above.

        Returns:
            List of Timeseries generator.start_stop_cost_time_series.

        Examples:

            List generator.start_stop_cost_time_series and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> generators = client.generator.start_stop_cost_time_series.list(limit=5)

        """
        filter_ = _create_generator_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            asset_type,
            asset_type_prefix,
            min_production_min,
            max_production_min,
            min_production_max,
            max_production_max,
            min_penstock_number,
            max_penstock_number,
            min_start_stop_cost,
            max_start_stop_cost,
            generator_efficiency_curve,
            external_id_prefix,
            space,
            filter,
        )
        external_ids = _retrieve_timeseries_external_ids_with_extra_start_stop_cost_time_series(self._client, self._view_id, filter_, limit)
        if external_ids:
            return self._client.time_series.retrieve_multiple(external_ids=list(external_ids))
        else:
            return TimeSeriesList([])


def _retrieve_timeseries_external_ids_with_extra_start_stop_cost_time_series(
    client: CogniteClient,
    view_id: dm.ViewId,
    filter_: dm.Filter | None,
    limit: int,
    extra_properties: ColumnNames | list[ColumnNames] = "startStopCostTimeSeries",
) -> dict[str, list[str]]:
    properties = {"startStopCostTimeSeries"}
    if isinstance(extra_properties, str):
        properties.add(extra_properties)
        extra_properties_list = [extra_properties]
    elif isinstance(extra_properties, list):
        properties.update(extra_properties)
        extra_properties_list = extra_properties
    else:
        raise ValueError(f"Invalid value for extra_properties: {extra_properties}")

    has_data = dm.filters.HasData(views=[view_id])
    has_property = dm.filters.Exists(property=view_id.as_property_ref("startStopCostTimeSeries"))
    filter_ = dm.filters.And(filter_, has_data, has_property) if filter_ else dm.filters.And(has_data, has_property)

    builder = QueryBuilder()
    builder.append(
        QueryStep(
            name="nodes",
            expression=dm.query.NodeResultSetExpression(filter=filter_),
            max_retrieve_limit=limit,
            select=dm.query.Select([dm.query.SourceSelector(view_id, list(properties))]),
        )
    )
    builder.execute_query(client)

    output: dict[str, list[str]] = {}
    for node in builder[0].results:
        if node.properties is None:
            continue
        view_prop = node.properties[view_id]
        key = view_prop["startStopCostTimeSeries"]
        values = [prop_ for prop in extra_properties_list if isinstance(prop_:= view_prop.get(prop, "MISSING"), str)]
        if isinstance(key, str):
            output[key] = values
        elif isinstance(key, list):
            for k in key:
                if isinstance(k, str):
                    output[k] = values
    return output
