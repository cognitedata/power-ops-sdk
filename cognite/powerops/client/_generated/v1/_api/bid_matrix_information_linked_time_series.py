from __future__ import annotations

import datetime
import warnings
from collections.abc import Sequence
from typing import Literal, cast

import pandas as pd
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import Datapoints, DatapointsArrayList, DatapointsList, TimeSeriesList
from cognite.client.data_classes.datapoints import Aggregate
from cognite.powerops.client._generated.v1.data_classes._bid_matrix_information import _create_bid_matrix_information_filter
from cognite.powerops.client._generated.v1.data_classes._core import QueryStep, DataClassQueryBuilder, DomainModelList
from cognite.powerops.client._generated.v1._api._core import DEFAULT_LIMIT_READ


ColumnNames = Literal["state", "bidMatrix", "linkedTimeSeries"]

class BidMatrixInformationLinkedTimeSeriesQuery:
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
        """`Retrieve datapoints for the `bid_matrix_information.linked_time_series` timeseries.

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
                >>> bid_matrix_information_datapoints = client.bid_matrix_information.linked_time_series(external_id="my_linked_time_series").retrieve(start="2w-ago")
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
        """`Retrieve numpy arrays for the `bid_matrix_information.linked_time_series` timeseries.

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
                >>> bid_matrix_information_datapoints = client.bid_matrix_information.linked_time_series(external_id="my_linked_time_series").retrieve_array(start="2w-ago")
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
        """`Retrieve DataFrames for the `bid_matrix_information.linked_time_series` timeseries.

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
                >>> bid_matrix_information_datapoints = client.bid_matrix_information.linked_time_series(external_id="my_linked_time_series").retrieve_dataframe(start="2w-ago")
        """
        external_ids = self._retrieve_timeseries_external_ids_with_extra(column_names)
        if external_ids:
            df = self._client.time_series.data.retrieve_dataframe(
                external_id=list(external_ids),
                start=start,
                end=end,
                aggregates=aggregates,  # type: ignore[arg-type]
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
        """Retrieve DataFrames for the `bid_matrix_information.linked_time_series` timeseries in Timezone.

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
                >>> bid_matrix_information_datapoints = client.bid_matrix_information.linked_time_series(
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
            df.columns = ["-".join(external_ids[external_id]) for external_id in df.columns]  # type: ignore[assignment]
        else:
            column_parts = (col.rsplit("|", maxsplit=splits) for col in df.columns)
            df.columns = [  # type: ignore[assignment]
                "-".join(external_ids[external_id]) + "|" + "|".join(parts) for external_id, *parts in column_parts
            ]
        return df


class BidMatrixInformationLinkedTimeSeriesAPI:
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        self._client = client
        self._view_id = view_id

    def __call__(
        self,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidMatrixInformationLinkedTimeSeriesQuery:
        """Query timeseries `bid_matrix_information.linked_time_series`

        Args:
            state: The state to filter on.
            state_prefix: The prefix of the state to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid matrix information to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query object that can be used to retrieve datapoins for the bid_matrix_information.linked_time_series timeseries
            selected in this method.

        Examples:

            Retrieve all data for 5 bid_matrix_information.linked_time_series timeseries:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_matrix_information_list = client.bid_matrix_information.linked_time_series(limit=5).retrieve()

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. "
            "Use the .select()...data.retrieve_dataframe() method instead.",
            UserWarning,
            stacklevel=2,
        )
        filter_ = _create_bid_matrix_information_filter(
            self._view_id,
            state,
            state_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return BidMatrixInformationLinkedTimeSeriesQuery(
            client=self._client,
            view_id=self._view_id,
            timeseries_limit=limit,
            filter=filter_,
        )

    def list(
        self,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TimeSeriesList:
        """List timeseries `bid_matrix_information.linked_time_series`

        Args:
            state: The state to filter on.
            state_prefix: The prefix of the state to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid matrix information to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of Timeseries bid_matrix_information.linked_time_series.

        Examples:

            List bid_matrix_information.linked_time_series and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_matrix_information_list = client.bid_matrix_information.linked_time_series.list(limit=5)

        """
        filter_ = _create_bid_matrix_information_filter(
            self._view_id,
            state,
            state_prefix,
            external_id_prefix,
            space,
            filter,
        )
        external_ids = _retrieve_timeseries_external_ids_with_extra_linked_time_series(self._client, self._view_id, filter_, limit)
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
    properties = {"linkedTimeSeries"}
    if isinstance(extra_properties, str):
        properties.add(extra_properties)
        extra_properties_list = [extra_properties]
    elif isinstance(extra_properties, list):
        properties.update(extra_properties)
        extra_properties_list = extra_properties
    else:
        raise ValueError(f"Invalid value for extra_properties: {extra_properties}")

    has_data = dm.filters.HasData(views=[view_id])
    has_property = dm.filters.Exists(property=view_id.as_property_ref("linkedTimeSeries"))
    filter_ = dm.filters.And(filter_, has_data, has_property) if filter_ else dm.filters.And(has_data, has_property)

    builder = DataClassQueryBuilder[DomainModelList](None)
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
        key = view_prop["linkedTimeSeries"]
        values = [prop_ for prop in extra_properties_list if isinstance(prop_:= view_prop.get(prop, "MISSING"), str)]
        if isinstance(key, str):
            output[key] = values
        elif isinstance(key, list):
            for k in key:
                if isinstance(k, str):
                    output[k] = values
    return output
