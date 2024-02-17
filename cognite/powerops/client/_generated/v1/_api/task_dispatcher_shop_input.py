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
    TaskDispatcherShopInput,
    TaskDispatcherShopInputWrite,
    TaskDispatcherShopInputFields,
    TaskDispatcherShopInputList,
    TaskDispatcherShopInputWriteList,
    TaskDispatcherShopInputTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._task_dispatcher_shop_input import (
    _TASKDISPATCHERSHOPINPUT_PROPERTIES_BY_FIELD,
    _create_task_dispatcher_shop_input_filter,
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
from .task_dispatcher_shop_input_query import TaskDispatcherShopInputQueryAPI


class TaskDispatcherShopInputAPI(
    NodeAPI[TaskDispatcherShopInput, TaskDispatcherShopInputWrite, TaskDispatcherShopInputList]
):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[TaskDispatcherShopInput]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=TaskDispatcherShopInput,
            class_list=TaskDispatcherShopInputList,
            class_write_list=TaskDispatcherShopInputWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        process_id: str | list[str] | None = None,
        process_id_prefix: str | None = None,
        min_process_step: int | None = None,
        max_process_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_bid_date: datetime.date | None = None,
        max_bid_date: datetime.date | None = None,
        min_shop_start: datetime.date | None = None,
        max_shop_start: datetime.date | None = None,
        min_shop_end: datetime.date | None = None,
        max_shop_end: datetime.date | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> TaskDispatcherShopInputQueryAPI[TaskDispatcherShopInputList]:
        """Query starting at task dispatcher shop inputs.

        Args:
            process_id: The process id to filter on.
            process_id_prefix: The prefix of the process id to filter on.
            min_process_step: The minimum value of the process step to filter on.
            max_process_step: The maximum value of the process step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            bid_configuration: The bid configuration to filter on.
            min_bid_date: The minimum value of the bid date to filter on.
            max_bid_date: The maximum value of the bid date to filter on.
            min_shop_start: The minimum value of the shop start to filter on.
            max_shop_start: The maximum value of the shop start to filter on.
            min_shop_end: The minimum value of the shop end to filter on.
            max_shop_end: The maximum value of the shop end to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of task dispatcher shop inputs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for task dispatcher shop inputs.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_task_dispatcher_shop_input_filter(
            self._view_id,
            process_id,
            process_id_prefix,
            min_process_step,
            max_process_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            bid_configuration,
            min_bid_date,
            max_bid_date,
            min_shop_start,
            max_shop_start,
            min_shop_end,
            max_shop_end,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(TaskDispatcherShopInputList)
        return TaskDispatcherShopInputQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        task_dispatcher_shop_input: TaskDispatcherShopInputWrite | Sequence[TaskDispatcherShopInputWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) task dispatcher shop inputs.

        Args:
            task_dispatcher_shop_input: Task dispatcher shop input or sequence of task dispatcher shop inputs to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new task_dispatcher_shop_input:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import TaskDispatcherShopInputWrite
                >>> client = PowerOpsModelsV1Client()
                >>> task_dispatcher_shop_input = TaskDispatcherShopInputWrite(external_id="my_task_dispatcher_shop_input", ...)
                >>> result = client.task_dispatcher_shop_input.apply(task_dispatcher_shop_input)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.task_dispatcher_shop_input.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(task_dispatcher_shop_input, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more task dispatcher shop input.

        Args:
            external_id: External id of the task dispatcher shop input to delete.
            space: The space where all the task dispatcher shop input are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete task_dispatcher_shop_input by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.task_dispatcher_shop_input.delete("my_task_dispatcher_shop_input")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.task_dispatcher_shop_input.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> TaskDispatcherShopInput | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> TaskDispatcherShopInputList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> TaskDispatcherShopInput | TaskDispatcherShopInputList | None:
        """Retrieve one or more task dispatcher shop inputs by id(s).

        Args:
            external_id: External id or list of external ids of the task dispatcher shop inputs.
            space: The space where all the task dispatcher shop inputs are located.

        Returns:
            The requested task dispatcher shop inputs.

        Examples:

            Retrieve task_dispatcher_shop_input by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> task_dispatcher_shop_input = client.task_dispatcher_shop_input.retrieve("my_task_dispatcher_shop_input")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: TaskDispatcherShopInputTextFields | Sequence[TaskDispatcherShopInputTextFields] | None = None,
        process_id: str | list[str] | None = None,
        process_id_prefix: str | None = None,
        min_process_step: int | None = None,
        max_process_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_bid_date: datetime.date | None = None,
        max_bid_date: datetime.date | None = None,
        min_shop_start: datetime.date | None = None,
        max_shop_start: datetime.date | None = None,
        min_shop_end: datetime.date | None = None,
        max_shop_end: datetime.date | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TaskDispatcherShopInputList:
        """Search task dispatcher shop inputs

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            process_id: The process id to filter on.
            process_id_prefix: The prefix of the process id to filter on.
            min_process_step: The minimum value of the process step to filter on.
            max_process_step: The maximum value of the process step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            bid_configuration: The bid configuration to filter on.
            min_bid_date: The minimum value of the bid date to filter on.
            max_bid_date: The maximum value of the bid date to filter on.
            min_shop_start: The minimum value of the shop start to filter on.
            max_shop_start: The maximum value of the shop start to filter on.
            min_shop_end: The minimum value of the shop end to filter on.
            max_shop_end: The maximum value of the shop end to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of task dispatcher shop inputs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results task dispatcher shop inputs matching the query.

        Examples:

           Search for 'my_task_dispatcher_shop_input' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> task_dispatcher_shop_inputs = client.task_dispatcher_shop_input.search('my_task_dispatcher_shop_input')

        """
        filter_ = _create_task_dispatcher_shop_input_filter(
            self._view_id,
            process_id,
            process_id_prefix,
            min_process_step,
            max_process_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            bid_configuration,
            min_bid_date,
            max_bid_date,
            min_shop_start,
            max_shop_start,
            min_shop_end,
            max_shop_end,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _TASKDISPATCHERSHOPINPUT_PROPERTIES_BY_FIELD, properties, filter_, limit
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
        property: TaskDispatcherShopInputFields | Sequence[TaskDispatcherShopInputFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: (
            TaskDispatcherShopInputTextFields | Sequence[TaskDispatcherShopInputTextFields] | None
        ) = None,
        process_id: str | list[str] | None = None,
        process_id_prefix: str | None = None,
        min_process_step: int | None = None,
        max_process_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_bid_date: datetime.date | None = None,
        max_bid_date: datetime.date | None = None,
        min_shop_start: datetime.date | None = None,
        max_shop_start: datetime.date | None = None,
        min_shop_end: datetime.date | None = None,
        max_shop_end: datetime.date | None = None,
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
        property: TaskDispatcherShopInputFields | Sequence[TaskDispatcherShopInputFields] | None = None,
        group_by: TaskDispatcherShopInputFields | Sequence[TaskDispatcherShopInputFields] = None,
        query: str | None = None,
        search_properties: (
            TaskDispatcherShopInputTextFields | Sequence[TaskDispatcherShopInputTextFields] | None
        ) = None,
        process_id: str | list[str] | None = None,
        process_id_prefix: str | None = None,
        min_process_step: int | None = None,
        max_process_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_bid_date: datetime.date | None = None,
        max_bid_date: datetime.date | None = None,
        min_shop_start: datetime.date | None = None,
        max_shop_start: datetime.date | None = None,
        min_shop_end: datetime.date | None = None,
        max_shop_end: datetime.date | None = None,
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
        property: TaskDispatcherShopInputFields | Sequence[TaskDispatcherShopInputFields] | None = None,
        group_by: TaskDispatcherShopInputFields | Sequence[TaskDispatcherShopInputFields] | None = None,
        query: str | None = None,
        search_property: TaskDispatcherShopInputTextFields | Sequence[TaskDispatcherShopInputTextFields] | None = None,
        process_id: str | list[str] | None = None,
        process_id_prefix: str | None = None,
        min_process_step: int | None = None,
        max_process_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_bid_date: datetime.date | None = None,
        max_bid_date: datetime.date | None = None,
        min_shop_start: datetime.date | None = None,
        max_shop_start: datetime.date | None = None,
        min_shop_end: datetime.date | None = None,
        max_shop_end: datetime.date | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across task dispatcher shop inputs

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            process_id: The process id to filter on.
            process_id_prefix: The prefix of the process id to filter on.
            min_process_step: The minimum value of the process step to filter on.
            max_process_step: The maximum value of the process step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            bid_configuration: The bid configuration to filter on.
            min_bid_date: The minimum value of the bid date to filter on.
            max_bid_date: The maximum value of the bid date to filter on.
            min_shop_start: The minimum value of the shop start to filter on.
            max_shop_start: The maximum value of the shop start to filter on.
            min_shop_end: The minimum value of the shop end to filter on.
            max_shop_end: The maximum value of the shop end to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of task dispatcher shop inputs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count task dispatcher shop inputs in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.task_dispatcher_shop_input.aggregate("count", space="my_space")

        """

        filter_ = _create_task_dispatcher_shop_input_filter(
            self._view_id,
            process_id,
            process_id_prefix,
            min_process_step,
            max_process_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            bid_configuration,
            min_bid_date,
            max_bid_date,
            min_shop_start,
            max_shop_start,
            min_shop_end,
            max_shop_end,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _TASKDISPATCHERSHOPINPUT_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: TaskDispatcherShopInputFields,
        interval: float,
        query: str | None = None,
        search_property: TaskDispatcherShopInputTextFields | Sequence[TaskDispatcherShopInputTextFields] | None = None,
        process_id: str | list[str] | None = None,
        process_id_prefix: str | None = None,
        min_process_step: int | None = None,
        max_process_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_bid_date: datetime.date | None = None,
        max_bid_date: datetime.date | None = None,
        min_shop_start: datetime.date | None = None,
        max_shop_start: datetime.date | None = None,
        min_shop_end: datetime.date | None = None,
        max_shop_end: datetime.date | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for task dispatcher shop inputs

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            process_id: The process id to filter on.
            process_id_prefix: The prefix of the process id to filter on.
            min_process_step: The minimum value of the process step to filter on.
            max_process_step: The maximum value of the process step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            bid_configuration: The bid configuration to filter on.
            min_bid_date: The minimum value of the bid date to filter on.
            max_bid_date: The maximum value of the bid date to filter on.
            min_shop_start: The minimum value of the shop start to filter on.
            max_shop_start: The maximum value of the shop start to filter on.
            min_shop_end: The minimum value of the shop end to filter on.
            max_shop_end: The maximum value of the shop end to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of task dispatcher shop inputs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_task_dispatcher_shop_input_filter(
            self._view_id,
            process_id,
            process_id_prefix,
            min_process_step,
            max_process_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            bid_configuration,
            min_bid_date,
            max_bid_date,
            min_shop_start,
            max_shop_start,
            min_shop_end,
            max_shop_end,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _TASKDISPATCHERSHOPINPUT_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        process_id: str | list[str] | None = None,
        process_id_prefix: str | None = None,
        min_process_step: int | None = None,
        max_process_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_bid_date: datetime.date | None = None,
        max_bid_date: datetime.date | None = None,
        min_shop_start: datetime.date | None = None,
        max_shop_start: datetime.date | None = None,
        min_shop_end: datetime.date | None = None,
        max_shop_end: datetime.date | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TaskDispatcherShopInputList:
        """List/filter task dispatcher shop inputs

        Args:
            process_id: The process id to filter on.
            process_id_prefix: The prefix of the process id to filter on.
            min_process_step: The minimum value of the process step to filter on.
            max_process_step: The maximum value of the process step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            bid_configuration: The bid configuration to filter on.
            min_bid_date: The minimum value of the bid date to filter on.
            max_bid_date: The maximum value of the bid date to filter on.
            min_shop_start: The minimum value of the shop start to filter on.
            max_shop_start: The maximum value of the shop start to filter on.
            min_shop_end: The minimum value of the shop end to filter on.
            max_shop_end: The maximum value of the shop end to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of task dispatcher shop inputs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested task dispatcher shop inputs

        Examples:

            List task dispatcher shop inputs and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> task_dispatcher_shop_inputs = client.task_dispatcher_shop_input.list(limit=5)

        """
        filter_ = _create_task_dispatcher_shop_input_filter(
            self._view_id,
            process_id,
            process_id_prefix,
            min_process_step,
            max_process_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            bid_configuration,
            min_bid_date,
            max_bid_date,
            min_shop_start,
            max_shop_start,
            min_shop_end,
            max_shop_end,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
