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
    BidCalculationTask,
    BidCalculationTaskWrite,
    BidCalculationTaskFields,
    BidCalculationTaskList,
    BidCalculationTaskWriteList,
)
from cognite.powerops.client._generated.v1.data_classes._bid_calculation_task import (
    _BIDCALCULATIONTASK_PROPERTIES_BY_FIELD,
    _create_bid_calculation_task_filter,
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
from .bid_calculation_task_query import BidCalculationTaskQueryAPI


class BidCalculationTaskAPI(NodeAPI[BidCalculationTask, BidCalculationTaskWrite, BidCalculationTaskList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[BidCalculationTask]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BidCalculationTask,
            class_list=BidCalculationTaskList,
            class_write_list=BidCalculationTaskWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        plant: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_bid_date: datetime.date | None = None,
        max_bid_date: datetime.date | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> BidCalculationTaskQueryAPI[BidCalculationTaskList]:
        """Query starting at bid calculation tasks.

        Args:
            plant: The plant to filter on.
            min_bid_date: The minimum value of the bid date to filter on.
            max_bid_date: The maximum value of the bid date to filter on.
            price_area: The price area to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid calculation tasks to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for bid calculation tasks.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_bid_calculation_task_filter(
            self._view_id,
            plant,
            min_bid_date,
            max_bid_date,
            price_area,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(BidCalculationTaskList)
        return BidCalculationTaskQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        bid_calculation_task: BidCalculationTaskWrite | Sequence[BidCalculationTaskWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) bid calculation tasks.

        Args:
            bid_calculation_task: Bid calculation task or sequence of bid calculation tasks to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new bid_calculation_task:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import BidCalculationTaskWrite
                >>> client = PowerOpsModelsV1Client()
                >>> bid_calculation_task = BidCalculationTaskWrite(external_id="my_bid_calculation_task", ...)
                >>> result = client.bid_calculation_task.apply(bid_calculation_task)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.bid_calculation_task.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(bid_calculation_task, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more bid calculation task.

        Args:
            external_id: External id of the bid calculation task to delete.
            space: The space where all the bid calculation task are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete bid_calculation_task by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.bid_calculation_task.delete("my_bid_calculation_task")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.bid_calculation_task.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> BidCalculationTask | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> BidCalculationTaskList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> BidCalculationTask | BidCalculationTaskList | None:
        """Retrieve one or more bid calculation tasks by id(s).

        Args:
            external_id: External id or list of external ids of the bid calculation tasks.
            space: The space where all the bid calculation tasks are located.

        Returns:
            The requested bid calculation tasks.

        Examples:

            Retrieve bid_calculation_task by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_calculation_task = client.bid_calculation_task.retrieve("my_bid_calculation_task")

        """
        return self._retrieve(external_id, space)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: BidCalculationTaskFields | Sequence[BidCalculationTaskFields] | None = None,
        group_by: None = None,
        plant: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_bid_date: datetime.date | None = None,
        max_bid_date: datetime.date | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: BidCalculationTaskFields | Sequence[BidCalculationTaskFields] | None = None,
        group_by: BidCalculationTaskFields | Sequence[BidCalculationTaskFields] = None,
        plant: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_bid_date: datetime.date | None = None,
        max_bid_date: datetime.date | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: BidCalculationTaskFields | Sequence[BidCalculationTaskFields] | None = None,
        group_by: BidCalculationTaskFields | Sequence[BidCalculationTaskFields] | None = None,
        plant: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_bid_date: datetime.date | None = None,
        max_bid_date: datetime.date | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across bid calculation tasks

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            plant: The plant to filter on.
            min_bid_date: The minimum value of the bid date to filter on.
            max_bid_date: The maximum value of the bid date to filter on.
            price_area: The price area to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid calculation tasks to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count bid calculation tasks in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.bid_calculation_task.aggregate("count", space="my_space")

        """

        filter_ = _create_bid_calculation_task_filter(
            self._view_id,
            plant,
            min_bid_date,
            max_bid_date,
            price_area,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _BIDCALCULATIONTASK_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: BidCalculationTaskFields,
        interval: float,
        plant: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_bid_date: datetime.date | None = None,
        max_bid_date: datetime.date | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for bid calculation tasks

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            plant: The plant to filter on.
            min_bid_date: The minimum value of the bid date to filter on.
            max_bid_date: The maximum value of the bid date to filter on.
            price_area: The price area to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid calculation tasks to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_bid_calculation_task_filter(
            self._view_id,
            plant,
            min_bid_date,
            max_bid_date,
            price_area,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _BIDCALCULATIONTASK_PROPERTIES_BY_FIELD,
            None,
            None,
            limit,
            filter_,
        )

    def list(
        self,
        plant: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_bid_date: datetime.date | None = None,
        max_bid_date: datetime.date | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidCalculationTaskList:
        """List/filter bid calculation tasks

        Args:
            plant: The plant to filter on.
            min_bid_date: The minimum value of the bid date to filter on.
            max_bid_date: The maximum value of the bid date to filter on.
            price_area: The price area to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid calculation tasks to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested bid calculation tasks

        Examples:

            List bid calculation tasks and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_calculation_tasks = client.bid_calculation_task.list(limit=5)

        """
        filter_ = _create_bid_calculation_task_filter(
            self._view_id,
            plant,
            min_bid_date,
            max_bid_date,
            price_area,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
