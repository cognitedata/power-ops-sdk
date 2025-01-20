from __future__ import annotations

import datetime
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
    BenchmarkingShopCase,
    BenchmarkingShopCaseWrite,
    BenchmarkingShopCaseFields,
    BenchmarkingShopCaseList,
    BenchmarkingShopCaseWriteList,
    BenchmarkingShopCaseTextFields,
    ShopFile,
    ShopScenario,
)
from cognite.powerops.client._generated.v1.data_classes._benchmarking_shop_case import (
    BenchmarkingShopCaseQuery,
    _BENCHMARKINGSHOPCASE_PROPERTIES_BY_FIELD,
    _create_benchmarking_shop_case_filter,
)
from cognite.powerops.client._generated.v1._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite.powerops.client._generated.v1._api.benchmarking_shop_case_shop_files import BenchmarkingShopCaseShopFilesAPI
from cognite.powerops.client._generated.v1._api.benchmarking_shop_case_query import BenchmarkingShopCaseQueryAPI


class BenchmarkingShopCaseAPI(NodeAPI[BenchmarkingShopCase, BenchmarkingShopCaseWrite, BenchmarkingShopCaseList, BenchmarkingShopCaseWriteList]):
    _view_id = dm.ViewId("power_ops_core", "BenchmarkingShopCase", "1")
    _properties_by_field = _BENCHMARKINGSHOPCASE_PROPERTIES_BY_FIELD
    _class_type = BenchmarkingShopCase
    _class_list = BenchmarkingShopCaseList
    _class_write_list = BenchmarkingShopCaseWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.shop_files_edge = BenchmarkingShopCaseShopFilesAPI(client)

    def __call__(
        self,
        scenario: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        bid_source: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_bid_generated: datetime.datetime | None = None,
        max_bid_generated: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> BenchmarkingShopCaseQueryAPI[BenchmarkingShopCaseList]:
        """Query starting at benchmarking shop cases.

        Args:
            scenario: The scenario to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            bid_source: The bid source to filter on.
            min_delivery_date: The minimum value of the delivery date to filter on.
            max_delivery_date: The maximum value of the delivery date to filter on.
            min_bid_generated: The minimum value of the bid generated to filter on.
            max_bid_generated: The maximum value of the bid generated to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of benchmarking shop cases to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for benchmarking shop cases.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. "
            "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_benchmarking_shop_case_filter(
            self._view_id,
            scenario,
            min_start_time,
            max_start_time,
            min_end_time,
            max_end_time,
            bid_source,
            min_delivery_date,
            max_delivery_date,
            min_bid_generated,
            max_bid_generated,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(BenchmarkingShopCaseList)
        return BenchmarkingShopCaseQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        benchmarking_shop_case: BenchmarkingShopCaseWrite | Sequence[BenchmarkingShopCaseWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) benchmarking shop cases.

        Note: This method iterates through all nodes and timeseries linked to benchmarking_shop_case and creates them including the edges
        between the nodes. For example, if any of `scenario` or `shop_files` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            benchmarking_shop_case: Benchmarking shop case or sequence of benchmarking shop cases to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new benchmarking_shop_case:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import BenchmarkingShopCaseWrite
                >>> client = PowerOpsModelsV1Client()
                >>> benchmarking_shop_case = BenchmarkingShopCaseWrite(external_id="my_benchmarking_shop_case", ...)
                >>> result = client.benchmarking_shop_case.apply(benchmarking_shop_case)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.benchmarking_shop_case.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(benchmarking_shop_case, replace, write_none)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> dm.InstancesDeleteResult:
        """Delete one or more benchmarking shop case.

        Args:
            external_id: External id of the benchmarking shop case to delete.
            space: The space where all the benchmarking shop case are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete benchmarking_shop_case by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.benchmarking_shop_case.delete("my_benchmarking_shop_case")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.benchmarking_shop_case.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE) -> BenchmarkingShopCase | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE) -> BenchmarkingShopCaseList:
        ...

    def retrieve(self, external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE) -> BenchmarkingShopCase | BenchmarkingShopCaseList | None:
        """Retrieve one or more benchmarking shop cases by id(s).

        Args:
            external_id: External id or list of external ids of the benchmarking shop cases.
            space: The space where all the benchmarking shop cases are located.

        Returns:
            The requested benchmarking shop cases.

        Examples:

            Retrieve benchmarking_shop_case by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> benchmarking_shop_case = client.benchmarking_shop_case.retrieve("my_benchmarking_shop_case")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.shop_files_edge,
                    "shop_files",
                    dm.DirectRelationReference("power_ops_types", "ShopCase.shopFiles"),
                    "outwards",
                    dm.ViewId("power_ops_core", "ShopFile", "1"),
                ),
                                               ]
        )

    def search(
        self,
        query: str,
        properties: BenchmarkingShopCaseTextFields | SequenceNotStr[BenchmarkingShopCaseTextFields] | None = None,
        scenario: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        bid_source: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_bid_generated: datetime.datetime | None = None,
        max_bid_generated: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: BenchmarkingShopCaseFields | SequenceNotStr[BenchmarkingShopCaseFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> BenchmarkingShopCaseList:
        """Search benchmarking shop cases

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            scenario: The scenario to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            bid_source: The bid source to filter on.
            min_delivery_date: The minimum value of the delivery date to filter on.
            max_delivery_date: The maximum value of the delivery date to filter on.
            min_bid_generated: The minimum value of the bid generated to filter on.
            max_bid_generated: The maximum value of the bid generated to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of benchmarking shop cases to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results benchmarking shop cases matching the query.

        Examples:

           Search for 'my_benchmarking_shop_case' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> benchmarking_shop_cases = client.benchmarking_shop_case.search('my_benchmarking_shop_case')

        """
        filter_ = _create_benchmarking_shop_case_filter(
            self._view_id,
            scenario,
            min_start_time,
            max_start_time,
            min_end_time,
            max_end_time,
            bid_source,
            min_delivery_date,
            max_delivery_date,
            min_bid_generated,
            max_bid_generated,
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
        property: BenchmarkingShopCaseFields | SequenceNotStr[BenchmarkingShopCaseFields] | None = None,
        scenario: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        bid_source: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_bid_generated: datetime.datetime | None = None,
        max_bid_generated: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.AggregatedNumberedValue: ...

    @overload
    def aggregate(
        self,
        aggregate: SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: None = None,
        property: BenchmarkingShopCaseFields | SequenceNotStr[BenchmarkingShopCaseFields] | None = None,
        scenario: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        bid_source: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_bid_generated: datetime.datetime | None = None,
        max_bid_generated: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: BenchmarkingShopCaseFields | SequenceNotStr[BenchmarkingShopCaseFields],
        property: BenchmarkingShopCaseFields | SequenceNotStr[BenchmarkingShopCaseFields] | None = None,
        scenario: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        bid_source: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_bid_generated: datetime.datetime | None = None,
        max_bid_generated: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: BenchmarkingShopCaseFields | SequenceNotStr[BenchmarkingShopCaseFields] | None = None,
        property: BenchmarkingShopCaseFields | SequenceNotStr[BenchmarkingShopCaseFields] | None = None,
        scenario: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        bid_source: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_bid_generated: datetime.datetime | None = None,
        max_bid_generated: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across benchmarking shop cases

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            scenario: The scenario to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            bid_source: The bid source to filter on.
            min_delivery_date: The minimum value of the delivery date to filter on.
            max_delivery_date: The maximum value of the delivery date to filter on.
            min_bid_generated: The minimum value of the bid generated to filter on.
            max_bid_generated: The maximum value of the bid generated to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of benchmarking shop cases to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count benchmarking shop cases in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.benchmarking_shop_case.aggregate("count", space="my_space")

        """

        filter_ = _create_benchmarking_shop_case_filter(
            self._view_id,
            scenario,
            min_start_time,
            max_start_time,
            min_end_time,
            max_end_time,
            bid_source,
            min_delivery_date,
            max_delivery_date,
            min_bid_generated,
            max_bid_generated,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            aggregate=aggregate,
            group_by=group_by,  # type: ignore[arg-type]
            properties=property,  # type: ignore[arg-type]
            query=None,
            search_properties=None,
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: BenchmarkingShopCaseFields,
        interval: float,
        scenario: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        bid_source: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_bid_generated: datetime.datetime | None = None,
        max_bid_generated: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for benchmarking shop cases

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            scenario: The scenario to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            bid_source: The bid source to filter on.
            min_delivery_date: The minimum value of the delivery date to filter on.
            max_delivery_date: The maximum value of the delivery date to filter on.
            min_bid_generated: The minimum value of the bid generated to filter on.
            max_bid_generated: The maximum value of the bid generated to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of benchmarking shop cases to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_benchmarking_shop_case_filter(
            self._view_id,
            scenario,
            min_start_time,
            max_start_time,
            min_end_time,
            max_end_time,
            bid_source,
            min_delivery_date,
            max_delivery_date,
            min_bid_generated,
            max_bid_generated,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            property,
            interval,
            None,
            None,
            limit,
            filter_,
        )

    def query(self) -> BenchmarkingShopCaseQuery:
        """Start a query for benchmarking shop cases."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return BenchmarkingShopCaseQuery(self._client)

    def select(self) -> BenchmarkingShopCaseQuery:
        """Start selecting from benchmarking shop cases."""
        warnings.warn("The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2)
        return BenchmarkingShopCaseQuery(self._client)

    def list(
        self,
        scenario: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        bid_source: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_bid_generated: datetime.datetime | None = None,
        max_bid_generated: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: BenchmarkingShopCaseFields | Sequence[BenchmarkingShopCaseFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> BenchmarkingShopCaseList:
        """List/filter benchmarking shop cases

        Args:
            scenario: The scenario to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            bid_source: The bid source to filter on.
            min_delivery_date: The minimum value of the delivery date to filter on.
            max_delivery_date: The maximum value of the delivery date to filter on.
            min_bid_generated: The minimum value of the bid generated to filter on.
            max_bid_generated: The maximum value of the bid generated to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of benchmarking shop cases to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `scenario` and `shop_files` for the benchmarking shop cases. Defaults to 'skip'.
                'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested benchmarking shop cases

        Examples:

            List benchmarking shop cases and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> benchmarking_shop_cases = client.benchmarking_shop_case.list(limit=5)

        """
        filter_ = _create_benchmarking_shop_case_filter(
            self._view_id,
            scenario,
            min_start_time,
            max_start_time,
            min_end_time,
            max_end_time,
            bid_source,
            min_delivery_date,
            max_delivery_date,
            min_bid_generated,
            max_bid_generated,
            external_id_prefix,
            space,
            filter,
        )

        if retrieve_connections == "skip":
            return self._list(
                limit=limit,
                filter=filter_,
                sort_by=sort_by,  # type: ignore[arg-type]
                direction=direction,
                sort=sort,
            )

        builder = DataClassQueryBuilder(BenchmarkingShopCaseList)
        has_data = dm.filters.HasData(views=[self._view_id])
        builder.append(
            NodeQueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=dm.filters.And(filter_, has_data) if filter_ else has_data,
                    sort=self._create_sort(sort_by, direction, sort),  # type: ignore[arg-type]
                ),
                BenchmarkingShopCase,
                max_retrieve_limit=limit,
                raw_filter=filter_,
            )
        )
        from_root = builder.get_from()
        edge_shop_files = builder.create_name(from_root)
        builder.append(
            EdgeQueryStep(
                edge_shop_files,
                dm.query.EdgeResultSetExpression(
                    from_=from_root,
                    direction="outwards",
                    chain_to="destination",
                ),
            )
        )
        if retrieve_connections == "full":
            builder.append(
                NodeQueryStep(
                    builder.create_name( edge_shop_files),
                    dm.query.NodeResultSetExpression(
                        from_= edge_shop_files,
                        filter=dm.filters.HasData(views=[ShopFile._view_id]),
                    ),
                    ShopFile,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[ShopScenario._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("scenario"),
                    ),
                    ShopScenario,
                )
            )
        # We know that that all nodes are connected as it is not possible to filter on connections
        builder.execute_query(self._client, remove_not_connected=False)
        return builder.unpack()
