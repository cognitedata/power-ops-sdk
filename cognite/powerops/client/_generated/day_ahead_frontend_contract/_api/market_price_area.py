from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import Literal, overload

import pandas as pd
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import Datapoints, DatapointsArrayList, DatapointsList, TimeSeriesList
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList
from cognite.client.data_classes.datapoints import Aggregate

from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes import (
    DomainModelApply,
    MarketPriceArea,
    MarketPriceAreaApply,
    MarketPriceAreaApplyList,
    MarketPriceAreaFields,
    MarketPriceAreaList,
    MarketPriceAreaTextFields,
)
from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes._market_price_area import (
    _MARKETPRICEAREA_PROPERTIES_BY_FIELD,
)

from ._core import DEFAULT_LIMIT_READ, INSTANCE_QUERY_LIMIT, Aggregations, TypeAPI

ColumnNames = Literal["name", "priceArea", "timezone", "mainScenario"]


class MarketPriceAreaMainScenarioQuery:
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
        """`Retrieve datapoints for the `market_price_area.main_scenario` timeseries.

        **Performance guide**:
            In order to retrieve millions of datapoints as efficiently as possible, here are a few guidelines:

            1. For best speed, and significantly lower memory usage, consider using ``retrieve_arrays(...)`` which uses ``numpy.ndarrays`` for data storage.
            2. Only unlimited queries with (``limit=None``) are fetched in parallel so specifying a large finite ``limit`` like 1 million, comes with severe performance penalty as data is fetched serially.
            3. Try to avoid specifying `start` and `end` to be very far from the actual data: If you have data from 2000 to 2015, don't set start=0 (1970).

        Args:
            start: Inclusive start. Default: 1970-01-01 UTC.
            end: Exclusive end. Default: "now"
            aggregates: Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity The granularity to fetch aggregates at. e.g. '15s', '2h', '10d'. Default: None.
            target_unit: The unit_external_id of the data points returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system: The unit system of the data points returned. Cannot be used with target_unit.
            limit (int | None): Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points (bool): Whether to include outside points. Not allowed when fetching aggregates. Default: False

        Returns:
            A ``DatapointsList`` with the requested datapoints.

        Examples:

            In this example,
            we are using the time-ago format to get raw data for the 'my_main_scenario' from 2 weeks ago up until now::

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> market_price_area_datapoints = client.market_price_area.main_scenario(external_id="my_main_scenario").retrieve(start="2w-ago)
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
        """`Retrieve numpy arrays for the `market_price_area.main_scenario` timeseries.

        **Performance guide**:
            In order to retrieve millions of datapoints as efficiently as possible, here are a few guidelines:

            1. For best speed, and significantly lower memory usage, consider using ``retrieve_arrays(...)`` which uses ``numpy.ndarrays`` for data storage.
            2. Only unlimited queries with (``limit=None``) are fetched in parallel so specifying a large finite ``limit`` like 1 million, comes with severe performance penalty as data is fetched serially.
            3. Try to avoid specifying `start` and `end` to be very far from the actual data: If you have data from 2000 to 2015, don't set start=0 (1970).

        Args:
            start: Inclusive start. Default: 1970-01-01 UTC.
            end: Exclusive end. Default: "now"
            aggregates: Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity The granularity to fetch aggregates at. e.g. '15s', '2h', '10d'. Default: None.
            target_unit: The unit_external_id of the data points returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system: The unit system of the data points returned. Cannot be used with target_unit.
            limit (int | None): Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points (bool): Whether to include outside points. Not allowed when fetching aggregates. Default: False

        Returns:
            A ``DatapointsArrayList`` with the requested datapoints.

        Examples:

            In this example,
            we are using the time-ago format to get raw data for the 'my_main_scenario' from 2 weeks ago up until now::

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> market_price_area_datapoints = client.market_price_area.main_scenario(external_id="my_main_scenario").retrieve_array(start="2w-ago)
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
        column_names: ColumnNames | list[ColumnNames] = "mainScenario",
    ) -> pd.DataFrame:
        """`Retrieve DataFrames for the `market_price_area.main_scenario` timeseries.

        **Performance guide**:
            In order to retrieve millions of datapoints as efficiently as possible, here are a few guidelines:

            1. For best speed, and significantly lower memory usage, consider using ``retrieve_arrays(...)`` which uses ``numpy.ndarrays`` for data storage.
            2. Only unlimited queries with (``limit=None``) are fetched in parallel so specifying a large finite ``limit`` like 1 million, comes with severe performance penalty as data is fetched serially.
            3. Try to avoid specifying `start` and `end` to be very far from the actual data: If you have data from 2000 to 2015, don't set start=0 (1970).

        Args:
            start: Inclusive start. Default: 1970-01-01 UTC.
            end: Exclusive end. Default: "now"
            aggregates: Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity The granularity to fetch aggregates at. e.g. '15s', '2h', '10d'. Default: None.
            target_unit: The unit_external_id of the data points returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system: The unit system of the data points returned. Cannot be used with target_unit.
            limit: Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points: Whether to include outside points. Not allowed when fetching aggregates. Default: False
            uniform_index: If only querying aggregates AND a single granularity is used AND no limit is used, specifying `uniform_index=True` will return a dataframe with an equidistant datetime index from the earliest `start` to the latest `end` (missing values will be NaNs). If these requirements are not met, a ValueError is raised. Default: False
            include_aggregate_name: Include 'aggregate' in the column name, e.g. `my-ts|average`. Ignored for raw time series. Default: True
            include_granularity_name: Include 'granularity' in the column name, e.g. `my-ts|12h`. Added after 'aggregate' when present. Ignored for raw time series. Default: False
            column_names: Which property to use for column names. Defauts to mainScenario


        Returns:
            A ``DataFrame`` with the requested datapoints.

        Examples:

            In this example,
            we are using the time-ago format to get raw data for the 'my_main_scenario' from 2 weeks ago up until now::

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> market_price_area_datapoints = client.market_price_area.main_scenario(external_id="my_main_scenario").retrieve_dataframe(start="2w-ago)
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
        column_names: ColumnNames | list[ColumnNames] = "mainScenario",
    ) -> pd.DataFrame:
        """Retrieve DataFrames for the `market_price_area.main_scenario` timeseries in Timezone.

        **Performance guide**:
            In order to retrieve millions of datapoints as efficiently as possible, here are a few guidelines:

            1. For best speed, and significantly lower memory usage, consider using ``retrieve_arrays(...)`` which uses ``numpy.ndarrays`` for data storage.
            2. Only unlimited queries with (``limit=None``) are fetched in parallel so specifying a large finite ``limit`` like 1 million, comes with severe performance penalty as data is fetched serially.
            3. Try to avoid specifying `start` and `end` to be very far from the actual data: If you have data from 2000 to 2015, don't set start=0 (1970).

        Args:
            start: Inclusive start.
            end: Exclusive end
            aggregates: Single aggregate or list of aggregates to retrieve. Default: None (raw datapoints returned)
            granularity The granularity to fetch aggregates at. e.g. '15s', '2h', '10d'. Default: None.
            target_unit: The unit_external_id of the data points returned. If the time series does not have a unit_external_id that can be converted to the target_unit, an error will be returned. Cannot be used with target_unit_system.
            target_unit_system: The unit system of the data points returned. Cannot be used with target_unit.
            limit: Maximum number of datapoints to return for each time series. Default: None (no limit)
            include_outside_points: Whether to include outside points. Not allowed when fetching aggregates. Default: False
            uniform_index: If only querying aggregates AND a single granularity is used AND no limit is used, specifying `uniform_index=True` will return a dataframe with an equidistant datetime index from the earliest `start` to the latest `end` (missing values will be NaNs). If these requirements are not met, a ValueError is raised. Default: False
            include_aggregate_name: Include 'aggregate' in the column name, e.g. `my-ts|average`. Ignored for raw time series. Default: True
            include_granularity_name: Include 'granularity' in the column name, e.g. `my-ts|12h`. Added after 'aggregate' when present. Ignored for raw time series. Default: False
            column_names: Which property to use for column names. Defauts to mainScenario


        Returns:
            A ``DataFrame`` with the requested datapoints.

        Examples:

            In this example,
            get weekly aggregates for the 'my_main_scenario' for the first month of 2023 in Oslo time:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> from datetime import datetime, timezone
                >>> client = DayAheadFrontendContractAPI()
                >>> market_price_area_datapoints = client.market_price_area.main_scenario(
                ...     external_id="my_main_scenario").retrieve_dataframe_in_timezone(
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
        self, extra_properties: ColumnNames | list[ColumnNames] = "mainScenario"
    ) -> dict[str, list[str]]:
        return _retrieve_timeseries_external_ids_with_extra_main_scenario(
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
        if isinstance(column_names, str) and column_names == "mainScenario":
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


class MarketPriceAreaMainScenarioAPI:
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        self._client = client
        self._view_id = view_id

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        default_method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MarketPriceAreaMainScenarioQuery:
        """Query timeseries `market_price_area.main_scenario`

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            default_method: The default method to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of market price areas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query object that can be used to retrieve datapoins for the  market_price_area.main_scenario timeseries
            selected in this method.

        Examples:

            Retrieve all data for 5 market_price_area.main_scenario timeseries:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> market_price_areas = client.market_price_area.main_scenario(limit=5).retrieve()

        """
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            price_area,
            price_area_prefix,
            default_method,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return MarketPriceAreaMainScenarioQuery(
            client=self._client,
            view_id=self._view_id,
            timeseries_limit=limit,
            filter=filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        default_method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TimeSeriesList:
        """List timeseries `market_price_area.main_scenario`

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            default_method: The default method to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of market price areas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of Timeseries market_price_area.main_scenario.

        Examples:

            List market_price_area.main_scenario and limit to 5:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> market_price_areas = client.market_price_area.main_scenario.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            price_area,
            price_area_prefix,
            default_method,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        external_ids = _retrieve_timeseries_external_ids_with_extra_main_scenario(
            self._client, self._view_id, filter_, limit
        )
        if external_ids:
            return self._client.time_series.retrieve_multiple(external_ids=list(external_ids))
        else:
            return TimeSeriesList([])


def _retrieve_timeseries_external_ids_with_extra_main_scenario(
    client: CogniteClient,
    view_id: dm.ViewId,
    filter_: dm.Filter | None,
    limit: int,
    extra_properties: ColumnNames | list[ColumnNames] = "mainScenario",
) -> dict[str, list[str]]:
    properties = ["mainScenario"]
    if extra_properties == "mainScenario":
        ...
    elif isinstance(extra_properties, str) and extra_properties != "mainScenario":
        properties.append(extra_properties)
    elif isinstance(extra_properties, list):
        properties.extend([prop for prop in extra_properties if prop != "mainScenario"])
    else:
        raise ValueError(f"Invalid value for extra_properties: {extra_properties}")

    if isinstance(extra_properties, str):
        extra_list = [extra_properties]
    else:
        extra_list = extra_properties
    has_data = dm.filters.HasData([dm.ContainerId("dayAheadFrontendContractModel", "MarketPriceArea")], [view_id])
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
            node.properties[view_id]["mainScenario"]: [node.properties[view_id].get(prop, "") for prop in extra_list]
            for node in result.data["nodes"].data
        }
        total_retrieved += len(batch_external_ids)
        external_ids.update(batch_external_ids)
        cursor = result.cursors["nodes"]
        if total_retrieved >= limit or cursor is None:
            break
    return external_ids


class MarketPriceAreaAPI(TypeAPI[MarketPriceArea, MarketPriceAreaApply, MarketPriceAreaList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[MarketPriceAreaApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=MarketPriceArea,
            class_apply_type=MarketPriceAreaApply,
            class_list=MarketPriceAreaList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.main_scenario = MarketPriceAreaMainScenarioAPI(client, view_id)

    def apply(
        self, market_price_area: MarketPriceAreaApply | Sequence[MarketPriceAreaApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) market price areas.

        Args:
            market_price_area: Market price area or sequence of market price areas to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new market_price_area:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes import MarketPriceAreaApply
                >>> client = DayAheadFrontendContractAPI()
                >>> market_price_area = MarketPriceAreaApply(external_id="my_market_price_area", ...)
                >>> result = client.market_price_area.apply(market_price_area)

        """
        if isinstance(market_price_area, MarketPriceAreaApply):
            instances = market_price_area.to_instances_apply(self._view_by_write_class)
        else:
            instances = MarketPriceAreaApplyList(market_price_area).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(
        self, external_id: str | Sequence[str], space: str = "dayAheadFrontendContractModel"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more market price area.

        Args:
            external_id: External id of the market price area to delete.
            space: The space where all the market price area are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete market_price_area by id:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> client.market_price_area.delete("my_market_price_area")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> MarketPriceArea:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> MarketPriceAreaList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "dayAheadFrontendContractModel"
    ) -> MarketPriceArea | MarketPriceAreaList:
        """Retrieve one or more market price areas by id(s).

        Args:
            external_id: External id or list of external ids of the market price areas.
            space: The space where all the market price areas are located.

        Returns:
            The requested market price areas.

        Examples:

            Retrieve market_price_area by id:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> market_price_area = client.market_price_area.retrieve("my_market_price_area")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: MarketPriceAreaTextFields | Sequence[MarketPriceAreaTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        default_method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MarketPriceAreaList:
        """Search market price areas

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            default_method: The default method to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of market price areas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results market price areas matching the query.

        Examples:

           Search for 'my_market_price_area' in all text properties:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> market_price_areas = client.market_price_area.search('my_market_price_area')

        """
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            price_area,
            price_area_prefix,
            default_method,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _MARKETPRICEAREA_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: MarketPriceAreaFields | Sequence[MarketPriceAreaFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: MarketPriceAreaTextFields | Sequence[MarketPriceAreaTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        default_method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
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
        property: MarketPriceAreaFields | Sequence[MarketPriceAreaFields] | None = None,
        group_by: MarketPriceAreaFields | Sequence[MarketPriceAreaFields] = None,
        query: str | None = None,
        search_properties: MarketPriceAreaTextFields | Sequence[MarketPriceAreaTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        default_method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
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
        property: MarketPriceAreaFields | Sequence[MarketPriceAreaFields] | None = None,
        group_by: MarketPriceAreaFields | Sequence[MarketPriceAreaFields] | None = None,
        query: str | None = None,
        search_property: MarketPriceAreaTextFields | Sequence[MarketPriceAreaTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        default_method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across market price areas

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            default_method: The default method to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of market price areas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count market price areas in space `my_space`:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> result = client.market_price_area.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            price_area,
            price_area_prefix,
            default_method,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _MARKETPRICEAREA_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: MarketPriceAreaFields,
        interval: float,
        query: str | None = None,
        search_property: MarketPriceAreaTextFields | Sequence[MarketPriceAreaTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        default_method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for market price areas

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            default_method: The default method to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of market price areas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            price_area,
            price_area_prefix,
            default_method,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _MARKETPRICEAREA_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        default_method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MarketPriceAreaList:
        """List/filter market price areas

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            default_method: The default method to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of market price areas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested market price areas

        Examples:

            List market price areas and limit to 5:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> market_price_areas = client.market_price_area.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            price_area,
            price_area_prefix,
            default_method,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    price_area: str | list[str] | None = None,
    price_area_prefix: str | None = None,
    default_method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    timezone: str | list[str] | None = None,
    timezone_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if price_area and isinstance(price_area, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("priceArea"), value=price_area))
    if price_area and isinstance(price_area, list):
        filters.append(dm.filters.In(view_id.as_property_ref("priceArea"), values=price_area))
    if price_area_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("priceArea"), value=price_area_prefix))
    if default_method and isinstance(default_method, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("defaultMethod"),
                value={"space": "dayAheadFrontendContractModel", "externalId": default_method},
            )
        )
    if default_method and isinstance(default_method, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("defaultMethod"),
                value={"space": default_method[0], "externalId": default_method[1]},
            )
        )
    if default_method and isinstance(default_method, list) and isinstance(default_method[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("defaultMethod"),
                values=[{"space": "dayAheadFrontendContractModel", "externalId": item} for item in default_method],
            )
        )
    if default_method and isinstance(default_method, list) and isinstance(default_method[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("defaultMethod"),
                values=[{"space": item[0], "externalId": item[1]} for item in default_method],
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
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
