from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    BenchmarkingTaskDispatcherInputDayAhead,
    BenchmarkingTaskDispatcherInputDayAheadWrite,
    BenchmarkingTaskDispatcherInputDayAheadFields,
    BenchmarkingTaskDispatcherInputDayAheadList,
    BenchmarkingTaskDispatcherInputDayAheadWriteList,
    BenchmarkingTaskDispatcherInputDayAheadTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._benchmarking_task_dispatcher_input_day_ahead import (
    _BENCHMARKINGTASKDISPATCHERINPUTDAYAHEAD_PROPERTIES_BY_FIELD,
    _create_benchmarking_task_dispatcher_input_day_ahead_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_QUERY_LIMIT,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
    QueryStep,
    QueryBuilder,
)
from .benchmarking_task_dispatcher_input_day_ahead_query import BenchmarkingTaskDispatcherInputDayAheadQueryAPI


class BenchmarkingTaskDispatcherInputDayAheadAPI(
    NodeAPI[
        BenchmarkingTaskDispatcherInputDayAhead,
        BenchmarkingTaskDispatcherInputDayAheadWrite,
        BenchmarkingTaskDispatcherInputDayAheadList,
    ]
):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[BenchmarkingTaskDispatcherInputDayAhead]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BenchmarkingTaskDispatcherInputDayAhead,
            class_list=BenchmarkingTaskDispatcherInputDayAheadList,
            class_write_list=BenchmarkingTaskDispatcherInputDayAheadWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_workflow_step: int | None = None,
        max_workflow_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        benchmarking_config: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_delivery_date: datetime.datetime | None = None,
        max_delivery_date: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> BenchmarkingTaskDispatcherInputDayAheadQueryAPI[BenchmarkingTaskDispatcherInputDayAheadList]:
        """Query starting at benchmarking task dispatcher input day aheads.

        Args:
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            min_workflow_step: The minimum value of the workflow step to filter on.
            max_workflow_step: The maximum value of the workflow step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            benchmarking_config: The benchmarking config to filter on.
            min_delivery_date: The minimum value of the delivery date to filter on.
            max_delivery_date: The maximum value of the delivery date to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of benchmarking task dispatcher input day aheads to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for benchmarking task dispatcher input day aheads.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_benchmarking_task_dispatcher_input_day_ahead_filter(
            self._view_id,
            workflow_execution_id,
            workflow_execution_id_prefix,
            min_workflow_step,
            max_workflow_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            benchmarking_config,
            min_delivery_date,
            max_delivery_date,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(BenchmarkingTaskDispatcherInputDayAheadList)
        return BenchmarkingTaskDispatcherInputDayAheadQueryAPI(
            self._client, builder, self._view_by_read_class, filter_, limit
        )

    def apply(
        self,
        benchmarking_task_dispatcher_input_day_ahead: (
            BenchmarkingTaskDispatcherInputDayAheadWrite | Sequence[BenchmarkingTaskDispatcherInputDayAheadWrite]
        ),
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) benchmarking task dispatcher input day aheads.

        Args:
            benchmarking_task_dispatcher_input_day_ahead: Benchmarking task dispatcher input day ahead or sequence of benchmarking task dispatcher input day aheads to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new benchmarking_task_dispatcher_input_day_ahead:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import BenchmarkingTaskDispatcherInputDayAheadWrite
                >>> client = PowerOpsModelsV1Client()
                >>> benchmarking_task_dispatcher_input_day_ahead = BenchmarkingTaskDispatcherInputDayAheadWrite(external_id="my_benchmarking_task_dispatcher_input_day_ahead", ...)
                >>> result = client.benchmarking_task_dispatcher_input_day_ahead.apply(benchmarking_task_dispatcher_input_day_ahead)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.benchmarking_task_dispatcher_input_day_ahead.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(benchmarking_task_dispatcher_input_day_ahead, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more benchmarking task dispatcher input day ahead.

        Args:
            external_id: External id of the benchmarking task dispatcher input day ahead to delete.
            space: The space where all the benchmarking task dispatcher input day ahead are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete benchmarking_task_dispatcher_input_day_ahead by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.benchmarking_task_dispatcher_input_day_ahead.delete("my_benchmarking_task_dispatcher_input_day_ahead")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.benchmarking_task_dispatcher_input_day_ahead.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE
    ) -> BenchmarkingTaskDispatcherInputDayAhead | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> BenchmarkingTaskDispatcherInputDayAheadList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> BenchmarkingTaskDispatcherInputDayAhead | BenchmarkingTaskDispatcherInputDayAheadList | None:
        """Retrieve one or more benchmarking task dispatcher input day aheads by id(s).

        Args:
            external_id: External id or list of external ids of the benchmarking task dispatcher input day aheads.
            space: The space where all the benchmarking task dispatcher input day aheads are located.

        Returns:
            The requested benchmarking task dispatcher input day aheads.

        Examples:

            Retrieve benchmarking_task_dispatcher_input_day_ahead by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> benchmarking_task_dispatcher_input_day_ahead = client.benchmarking_task_dispatcher_input_day_ahead.retrieve("my_benchmarking_task_dispatcher_input_day_ahead")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: (
            BenchmarkingTaskDispatcherInputDayAheadTextFields
            | Sequence[BenchmarkingTaskDispatcherInputDayAheadTextFields]
            | None
        ) = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_workflow_step: int | None = None,
        max_workflow_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        benchmarking_config: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_delivery_date: datetime.datetime | None = None,
        max_delivery_date: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BenchmarkingTaskDispatcherInputDayAheadList:
        """Search benchmarking task dispatcher input day aheads

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            min_workflow_step: The minimum value of the workflow step to filter on.
            max_workflow_step: The maximum value of the workflow step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            benchmarking_config: The benchmarking config to filter on.
            min_delivery_date: The minimum value of the delivery date to filter on.
            max_delivery_date: The maximum value of the delivery date to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of benchmarking task dispatcher input day aheads to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results benchmarking task dispatcher input day aheads matching the query.

        Examples:

           Search for 'my_benchmarking_task_dispatcher_input_day_ahead' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> benchmarking_task_dispatcher_input_day_aheads = client.benchmarking_task_dispatcher_input_day_ahead.search('my_benchmarking_task_dispatcher_input_day_ahead')

        """
        filter_ = _create_benchmarking_task_dispatcher_input_day_ahead_filter(
            self._view_id,
            workflow_execution_id,
            workflow_execution_id_prefix,
            min_workflow_step,
            max_workflow_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            benchmarking_config,
            min_delivery_date,
            max_delivery_date,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id,
            query,
            _BENCHMARKINGTASKDISPATCHERINPUTDAYAHEAD_PROPERTIES_BY_FIELD,
            properties,
            filter_,
            limit,
        )

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: (
            BenchmarkingTaskDispatcherInputDayAheadFields
            | Sequence[BenchmarkingTaskDispatcherInputDayAheadFields]
            | None
        ) = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: (
            BenchmarkingTaskDispatcherInputDayAheadTextFields
            | Sequence[BenchmarkingTaskDispatcherInputDayAheadTextFields]
            | None
        ) = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_workflow_step: int | None = None,
        max_workflow_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        benchmarking_config: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_delivery_date: datetime.datetime | None = None,
        max_delivery_date: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: (
            BenchmarkingTaskDispatcherInputDayAheadFields
            | Sequence[BenchmarkingTaskDispatcherInputDayAheadFields]
            | None
        ) = None,
        group_by: (
            BenchmarkingTaskDispatcherInputDayAheadFields | Sequence[BenchmarkingTaskDispatcherInputDayAheadFields]
        ) = None,
        query: str | None = None,
        search_properties: (
            BenchmarkingTaskDispatcherInputDayAheadTextFields
            | Sequence[BenchmarkingTaskDispatcherInputDayAheadTextFields]
            | None
        ) = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_workflow_step: int | None = None,
        max_workflow_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        benchmarking_config: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_delivery_date: datetime.datetime | None = None,
        max_delivery_date: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: (
            BenchmarkingTaskDispatcherInputDayAheadFields
            | Sequence[BenchmarkingTaskDispatcherInputDayAheadFields]
            | None
        ) = None,
        group_by: (
            BenchmarkingTaskDispatcherInputDayAheadFields
            | Sequence[BenchmarkingTaskDispatcherInputDayAheadFields]
            | None
        ) = None,
        query: str | None = None,
        search_property: (
            BenchmarkingTaskDispatcherInputDayAheadTextFields
            | Sequence[BenchmarkingTaskDispatcherInputDayAheadTextFields]
            | None
        ) = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_workflow_step: int | None = None,
        max_workflow_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        benchmarking_config: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_delivery_date: datetime.datetime | None = None,
        max_delivery_date: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across benchmarking task dispatcher input day aheads

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            min_workflow_step: The minimum value of the workflow step to filter on.
            max_workflow_step: The maximum value of the workflow step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            benchmarking_config: The benchmarking config to filter on.
            min_delivery_date: The minimum value of the delivery date to filter on.
            max_delivery_date: The maximum value of the delivery date to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of benchmarking task dispatcher input day aheads to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count benchmarking task dispatcher input day aheads in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.benchmarking_task_dispatcher_input_day_ahead.aggregate("count", space="my_space")

        """

        filter_ = _create_benchmarking_task_dispatcher_input_day_ahead_filter(
            self._view_id,
            workflow_execution_id,
            workflow_execution_id_prefix,
            min_workflow_step,
            max_workflow_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            benchmarking_config,
            min_delivery_date,
            max_delivery_date,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _BENCHMARKINGTASKDISPATCHERINPUTDAYAHEAD_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: BenchmarkingTaskDispatcherInputDayAheadFields,
        interval: float,
        query: str | None = None,
        search_property: (
            BenchmarkingTaskDispatcherInputDayAheadTextFields
            | Sequence[BenchmarkingTaskDispatcherInputDayAheadTextFields]
            | None
        ) = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_workflow_step: int | None = None,
        max_workflow_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        benchmarking_config: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_delivery_date: datetime.datetime | None = None,
        max_delivery_date: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for benchmarking task dispatcher input day aheads

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            min_workflow_step: The minimum value of the workflow step to filter on.
            max_workflow_step: The maximum value of the workflow step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            benchmarking_config: The benchmarking config to filter on.
            min_delivery_date: The minimum value of the delivery date to filter on.
            max_delivery_date: The maximum value of the delivery date to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of benchmarking task dispatcher input day aheads to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_benchmarking_task_dispatcher_input_day_ahead_filter(
            self._view_id,
            workflow_execution_id,
            workflow_execution_id_prefix,
            min_workflow_step,
            max_workflow_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            benchmarking_config,
            min_delivery_date,
            max_delivery_date,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _BENCHMARKINGTASKDISPATCHERINPUTDAYAHEAD_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_workflow_step: int | None = None,
        max_workflow_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        benchmarking_config: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_delivery_date: datetime.datetime | None = None,
        max_delivery_date: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BenchmarkingTaskDispatcherInputDayAheadList:
        """List/filter benchmarking task dispatcher input day aheads

        Args:
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            min_workflow_step: The minimum value of the workflow step to filter on.
            max_workflow_step: The maximum value of the workflow step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            benchmarking_config: The benchmarking config to filter on.
            min_delivery_date: The minimum value of the delivery date to filter on.
            max_delivery_date: The maximum value of the delivery date to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of benchmarking task dispatcher input day aheads to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested benchmarking task dispatcher input day aheads

        Examples:

            List benchmarking task dispatcher input day aheads and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> benchmarking_task_dispatcher_input_day_aheads = client.benchmarking_task_dispatcher_input_day_ahead.list(limit=5)

        """
        filter_ = _create_benchmarking_task_dispatcher_input_day_ahead_filter(
            self._view_id,
            workflow_execution_id,
            workflow_execution_id_prefix,
            min_workflow_step,
            max_workflow_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            benchmarking_config,
            min_delivery_date,
            max_delivery_date,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
