from collections.abc import Callable

import pandas as pd
import datetime
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling.ids import NodeId
from cognite.client.data_classes.datapoints import Aggregate
from cognite.client.utils._time import ZoneInfo

from cognite.powerops.client._generated.v1.data_classes._core.constants import DEFAULT_QUERY_LIMIT


class DataPointsAPI:
    def __init__(self, client: CogniteClient, get_node_ids: Callable[[int], list[NodeId]]) -> None:
        self._client = client
        self._get_node_ids = get_node_ids

    def retrieve_dataframe(
        self,
        start: int | str | datetime.datetime | None = None,
        end: int | str | datetime.datetime | None = None,
        aggregates: Aggregate | str | list[Aggregate | str] | None = None,
        granularity: str | None = None,
        timezone: str | datetime.timezone | ZoneInfo | None = None,
        target_unit: str | None = None,
        target_unit_system: str | None = None,
        limit: int | None = None,
        timeseries_limit: int = DEFAULT_QUERY_LIMIT,
        include_outside_points: bool = False,
        ignore_unknown_ids: bool = False,
        include_status: bool = False,
        ignore_bad_datapoints: bool = True,
        treat_uncertain_as_bad: bool = True,
        uniform_index: bool = False,
        include_aggregate_name: bool = True,
        include_granularity_name: bool = False,
    ) -> pd.DataFrame:
        """Get datapoints directly in a pandas dataframe.

        Time series support status codes like Good, Uncertain and Bad. You can read more in the Cognite Data Fusion developer documentation on
        `status codes. <https://developer.cognite.com/dev/concepts/reference/quality_codes/>`_

        Note:
            For many more usage examples, check out the :py:meth:`~DatapointsAPI.retrieve` method which accepts exactly the same arguments.

        Args:
            start (int | str | datetime.datetime | None): Inclusive start. Default: 1970-01-01 UTC.
            end (int | str | datetime.datetime | None): Exclusive end. Default: "now"
            aggregates (Aggregate | str | list[Aggregate | str] | None): Single aggregate or list of aggregates to retrieve. Available options: ``average``, ``continuous_variance``, ``count``, ``count_bad``, ``count_good``, ``count_uncertain``, ``discrete_variance``, ``duration_bad``, ``duration_good``, ``duration_uncertain``, ``interpolation``, ``max``, ``min``, ``step_interpolation``, ``sum`` and ``total_variation``. Default: None (raw datapoints returned)
            granularity (str | None): The granularity to fetch aggregates at. Can be given as an abbreviation or spelled out for clarity: ``s/second(s)``, ``m/minute(s)``, ``h/hour(s)``, ``d/day(s)``, ``w/week(s)``, ``mo/month(s)``, ``q/quarter(s)``, or ``y/year(s)``. Examples: ``30s``, ``5m``, ``1day``, ``2weeks``. Default: None.
            timezone (str | datetime.timezone | ZoneInfo | None): For raw datapoints, which timezone to use when displaying (will not affect what is retrieved). For aggregates, which timezone to align to for granularity 'hour' and longer. Align to the start of the hour, -day or -month. For timezones of type Region/Location, like 'Europe/Oslo', pass a string or ``ZoneInfo`` instance. The aggregate duration will then vary, typically due to daylight saving time. You can also use a fixed offset from UTC by passing a string like '+04:00', 'UTC-7' or 'UTC-02:30' or an instance of ``datetime.timezone``. Note: Historical timezones with second offset are not supported, and timezones with minute offsets (e.g. UTC+05:30 or Asia/Kolkata) may take longer to execute.
            target_unit (str | None): The unit_external_id of the datapoints returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system (str | None): The unit system of the datapoints returned. Cannot be used with target_unit.
            limit (int | None): Maximum number of datapoints to return for each time series. Default: None (no limit)
            timeseries_limit (int): Maximum number of timeseries to fetch (columns in the dataframe). Default: 5
            include_outside_points (bool): Whether to include outside points. Not allowed when fetching aggregates. Default: False
            ignore_unknown_ids (bool): Whether to ignore missing time series rather than raising an exception. Default: False
            include_status (bool): Also return the status code, an integer, for each datapoint in the response. Only relevant for raw datapoint queries, not aggregates.
            ignore_bad_datapoints (bool): Treat datapoints with a bad status code as if they do not exist. If set to false, raw queries will include bad datapoints in the response, and aggregates will in general omit the time period between a bad datapoint and the next good datapoint. Also, the period between a bad datapoint and the previous good datapoint will be considered constant. Default: True.
            treat_uncertain_as_bad (bool): Treat datapoints with uncertain status codes as bad. If false, treat datapoints with uncertain status codes as good. Used for both raw queries and aggregates. Default: True.
            uniform_index (bool): If only querying aggregates AND a single granularity is used AND no limit is used, specifying `uniform_index=True` will return a dataframe with an equidistant datetime index from the earliest `start` to the latest `end` (missing values will be NaNs). If these requirements are not met, a ValueError is raised. Default: False
            include_aggregate_name (bool): Include 'aggregate' in the column name, e.g. `my-ts|average`. Ignored for raw time series. Default: True
            include_granularity_name (bool): Include 'granularity' in the column name, e.g. `my-ts|12h`. Added after 'aggregate' when present. Ignored for raw time series. Default: False

        Returns:
            pd.DataFrame: A pandas DataFrame containing the requested time series. The ordering of columns is ids first, then external_ids. For time series with multiple aggregates, they will be sorted in alphabetical order ("average" before "max").

        Warning:
            If you have duplicated time series in your query, the dataframe columns will also contain duplicates.

            When retrieving raw datapoints with ``ignore_bad_datapoints=False``, bad datapoints with the value NaN can not be distinguished from those
            missing a value (due to being stored in a numpy array); all will become NaNs in the dataframe.
        """
        node_ids = self._get_node_ids(timeseries_limit)
        if not node_ids:
            return pd.DataFrame()
        return self._client.time_series.data.retrieve_dataframe(
            instance_id=node_ids,
            start=start,
            end=end,
            aggregates=aggregates,
            granularity=granularity,
            timezone=timezone,
            target_unit=target_unit,
            target_unit_system=target_unit_system,
            limit=limit,
            include_outside_points=include_outside_points,
            ignore_unknown_ids=ignore_unknown_ids,
            include_status=include_status,
            ignore_bad_datapoints=ignore_bad_datapoints,
            treat_uncertain_as_bad=treat_uncertain_as_bad,
            uniform_index=uniform_index,
            include_aggregate_name=include_aggregate_name,
            include_granularity_name=include_granularity_name,
            column_names="instance_id",
        )
