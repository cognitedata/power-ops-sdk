from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    NodeQueryStep,
    EdgeQueryStep,
    DataClassQueryBuilder,
)
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    BenchmarkingProductionObligationDayAhead,
    BenchmarkingProductionObligationDayAheadWrite,
    BenchmarkingProductionObligationDayAheadFields,
    BenchmarkingProductionObligationDayAheadList,
    BenchmarkingProductionObligationDayAheadWriteList,
    BenchmarkingProductionObligationDayAheadTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._benchmarking_production_obligation_day_ahead import (
    BenchmarkingProductionObligationDayAheadQuery,
    _BENCHMARKINGPRODUCTIONOBLIGATIONDAYAHEAD_PROPERTIES_BY_FIELD,
    _create_benchmarking_production_obligation_day_ahead_filter,
)
from cognite.powerops.client._generated.v1._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite.powerops.client._generated.v1._api.benchmarking_production_obligation_day_ahead_time_series import BenchmarkingProductionObligationDayAheadTimeSeriesAPI
from cognite.powerops.client._generated.v1._api.benchmarking_production_obligation_day_ahead_query import BenchmarkingProductionObligationDayAheadQueryAPI


class BenchmarkingProductionObligationDayAheadAPI(NodeAPI[BenchmarkingProductionObligationDayAhead, BenchmarkingProductionObligationDayAheadWrite, BenchmarkingProductionObligationDayAheadList, BenchmarkingProductionObligationDayAheadWriteList]):
    _view_id = dm.ViewId("power_ops_core", "BenchmarkingProductionObligationDayAhead", "1")
    _properties_by_field = _BENCHMARKINGPRODUCTIONOBLIGATIONDAYAHEAD_PROPERTIES_BY_FIELD
    _class_type = BenchmarkingProductionObligationDayAhead
    _class_list = BenchmarkingProductionObligationDayAheadList
    _class_write_list = BenchmarkingProductionObligationDayAheadWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.time_series = BenchmarkingProductionObligationDayAheadTimeSeriesAPI(client, self._view_id)

    def __call__(
            self,
            name: str | list[str] | None = None,
            name_prefix: str | None = None,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit: int = DEFAULT_QUERY_LIMIT,
            filter: dm.Filter | None = None,
    ) -> BenchmarkingProductionObligationDayAheadQueryAPI[BenchmarkingProductionObligationDayAheadList]:
        """Query starting at benchmarking production obligation day aheads.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of benchmarking production obligation day aheads to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for benchmarking production obligation day aheads.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. "
            "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_benchmarking_production_obligation_day_ahead_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(BenchmarkingProductionObligationDayAheadList)
        return BenchmarkingProductionObligationDayAheadQueryAPI(self._client, builder, filter_, limit)


    def apply(
        self,
        benchmarking_production_obligation_day_ahead: BenchmarkingProductionObligationDayAheadWrite | Sequence[BenchmarkingProductionObligationDayAheadWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) benchmarking production obligation day aheads.

        Args:
            benchmarking_production_obligation_day_ahead: Benchmarking production obligation day ahead or sequence of benchmarking production obligation day aheads to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new benchmarking_production_obligation_day_ahead:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import BenchmarkingProductionObligationDayAheadWrite
                >>> client = PowerOpsModelsV1Client()
                >>> benchmarking_production_obligation_day_ahead = BenchmarkingProductionObligationDayAheadWrite(external_id="my_benchmarking_production_obligation_day_ahead", ...)
                >>> result = client.benchmarking_production_obligation_day_ahead.apply(benchmarking_production_obligation_day_ahead)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.benchmarking_production_obligation_day_ahead.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(benchmarking_production_obligation_day_ahead, replace, write_none)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> dm.InstancesDeleteResult:
        """Delete one or more benchmarking production obligation day ahead.

        Args:
            external_id: External id of the benchmarking production obligation day ahead to delete.
            space: The space where all the benchmarking production obligation day ahead are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete benchmarking_production_obligation_day_ahead by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.benchmarking_production_obligation_day_ahead.delete("my_benchmarking_production_obligation_day_ahead")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.benchmarking_production_obligation_day_ahead.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE) -> BenchmarkingProductionObligationDayAhead | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE) -> BenchmarkingProductionObligationDayAheadList:
        ...

    def retrieve(self, external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE) -> BenchmarkingProductionObligationDayAhead | BenchmarkingProductionObligationDayAheadList | None:
        """Retrieve one or more benchmarking production obligation day aheads by id(s).

        Args:
            external_id: External id or list of external ids of the benchmarking production obligation day aheads.
            space: The space where all the benchmarking production obligation day aheads are located.

        Returns:
            The requested benchmarking production obligation day aheads.

        Examples:

            Retrieve benchmarking_production_obligation_day_ahead by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> benchmarking_production_obligation_day_ahead = client.benchmarking_production_obligation_day_ahead.retrieve("my_benchmarking_production_obligation_day_ahead")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: BenchmarkingProductionObligationDayAheadTextFields | SequenceNotStr[BenchmarkingProductionObligationDayAheadTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: BenchmarkingProductionObligationDayAheadFields | SequenceNotStr[BenchmarkingProductionObligationDayAheadFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> BenchmarkingProductionObligationDayAheadList:
        """Search benchmarking production obligation day aheads

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of benchmarking production obligation day aheads to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results benchmarking production obligation day aheads matching the query.

        Examples:

           Search for 'my_benchmarking_production_obligation_day_ahead' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> benchmarking_production_obligation_day_aheads = client.benchmarking_production_obligation_day_ahead.search('my_benchmarking_production_obligation_day_ahead')

        """
        filter_ = _create_benchmarking_production_obligation_day_ahead_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            query=query,
            properties=properties,
            filter_=filter_,
            limit=limit,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
        )

    @overload
    def aggregate(
        self,
        aggregate: Aggregations | dm.aggregations.MetricAggregation,
        group_by: None = None,
        property: BenchmarkingProductionObligationDayAheadFields | SequenceNotStr[BenchmarkingProductionObligationDayAheadFields] | None = None,
        query: str | None = None,
        search_property: BenchmarkingProductionObligationDayAheadTextFields | SequenceNotStr[BenchmarkingProductionObligationDayAheadTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.AggregatedNumberedValue:
        ...

    @overload
    def aggregate(
        self,
        aggregate: SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: None = None,
        property: BenchmarkingProductionObligationDayAheadFields | SequenceNotStr[BenchmarkingProductionObligationDayAheadFields] | None = None,
        query: str | None = None,
        search_property: BenchmarkingProductionObligationDayAheadTextFields | SequenceNotStr[BenchmarkingProductionObligationDayAheadTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: BenchmarkingProductionObligationDayAheadFields | SequenceNotStr[BenchmarkingProductionObligationDayAheadFields],
        property: BenchmarkingProductionObligationDayAheadFields | SequenceNotStr[BenchmarkingProductionObligationDayAheadFields] | None = None,
        query: str | None = None,
        search_property: BenchmarkingProductionObligationDayAheadTextFields | SequenceNotStr[BenchmarkingProductionObligationDayAheadTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: BenchmarkingProductionObligationDayAheadFields | SequenceNotStr[BenchmarkingProductionObligationDayAheadFields] | None = None,
        property: BenchmarkingProductionObligationDayAheadFields | SequenceNotStr[BenchmarkingProductionObligationDayAheadFields] | None = None,
        query: str | None = None,
        search_property: BenchmarkingProductionObligationDayAheadTextFields | SequenceNotStr[BenchmarkingProductionObligationDayAheadTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across benchmarking production obligation day aheads

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of benchmarking production obligation day aheads to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count benchmarking production obligation day aheads in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.benchmarking_production_obligation_day_ahead.aggregate("count", space="my_space")

        """

        filter_ = _create_benchmarking_production_obligation_day_ahead_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            aggregate=aggregate,
            group_by=group_by,  # type: ignore[arg-type]
            properties=property,  # type: ignore[arg-type]
            query=query,
            search_properties=search_property,  # type: ignore[arg-type]
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: BenchmarkingProductionObligationDayAheadFields,
        interval: float,
        query: str | None = None,
        search_property: BenchmarkingProductionObligationDayAheadTextFields | SequenceNotStr[BenchmarkingProductionObligationDayAheadTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for benchmarking production obligation day aheads

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of benchmarking production obligation day aheads to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_benchmarking_production_obligation_day_ahead_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            property,
            interval,
            query,
            search_property,  # type: ignore[arg-type]
            limit,
            filter_,
        )

    def query(self) -> BenchmarkingProductionObligationDayAheadQuery:
        """Start a query for benchmarking production obligation day aheads."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return BenchmarkingProductionObligationDayAheadQuery(self._client)

    def select(self) -> BenchmarkingProductionObligationDayAheadQuery:
        """Start selecting from benchmarking production obligation day aheads."""
        warnings.warn("The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2)
        return BenchmarkingProductionObligationDayAheadQuery(self._client)

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: BenchmarkingProductionObligationDayAheadFields | Sequence[BenchmarkingProductionObligationDayAheadFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> BenchmarkingProductionObligationDayAheadList:
        """List/filter benchmarking production obligation day aheads

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of benchmarking production obligation day aheads to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested benchmarking production obligation day aheads

        Examples:

            List benchmarking production obligation day aheads and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> benchmarking_production_obligation_day_aheads = client.benchmarking_production_obligation_day_ahead.list(limit=5)

        """
        filter_ = _create_benchmarking_production_obligation_day_ahead_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
        )
