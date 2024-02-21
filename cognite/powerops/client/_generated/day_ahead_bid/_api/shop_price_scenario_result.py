from __future__ import annotations

from collections.abc import Sequence
from typing import overload
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.day_ahead_bid.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.day_ahead_bid.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    SHOPPriceScenarioResult,
    SHOPPriceScenarioResultWrite,
    SHOPPriceScenarioResultFields,
    SHOPPriceScenarioResultList,
    SHOPPriceScenarioResultWriteList,
)
from cognite.powerops.client._generated.day_ahead_bid.data_classes._shop_price_scenario_result import (
    _SHOPPRICESCENARIORESULT_PROPERTIES_BY_FIELD,
    _create_shop_price_scenario_result_filter,
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
from .shop_price_scenario_result_price import SHOPPriceScenarioResultPriceAPI
from .shop_price_scenario_result_production import SHOPPriceScenarioResultProductionAPI
from .shop_price_scenario_result_query import SHOPPriceScenarioResultQueryAPI


class SHOPPriceScenarioResultAPI(
    NodeAPI[SHOPPriceScenarioResult, SHOPPriceScenarioResultWrite, SHOPPriceScenarioResultList]
):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[SHOPPriceScenarioResult]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=SHOPPriceScenarioResult,
            class_list=SHOPPriceScenarioResultList,
            class_write_list=SHOPPriceScenarioResultWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.price = SHOPPriceScenarioResultPriceAPI(client, view_id)
        self.production = SHOPPriceScenarioResultProductionAPI(client, view_id)

    def __call__(
        self,
        price_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> SHOPPriceScenarioResultQueryAPI[SHOPPriceScenarioResultList]:
        """Query starting at shop price scenario results.

        Args:
            price_scenario: The price scenario to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop price scenario results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for shop price scenario results.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_shop_price_scenario_result_filter(
            self._view_id,
            price_scenario,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(SHOPPriceScenarioResultList)
        return SHOPPriceScenarioResultQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        shop_price_scenario_result: SHOPPriceScenarioResultWrite | Sequence[SHOPPriceScenarioResultWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) shop price scenario results.

        Args:
            shop_price_scenario_result: Shop price scenario result or sequence of shop price scenario results to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new shop_price_scenario_result:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> from cognite.powerops.client._generated.day_ahead_bid.data_classes import SHOPPriceScenarioResultWrite
                >>> client = DayAheadBidAPI()
                >>> shop_price_scenario_result = SHOPPriceScenarioResultWrite(external_id="my_shop_price_scenario_result", ...)
                >>> result = client.shop_price_scenario_result.apply(shop_price_scenario_result)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.shop_price_scenario_result.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(shop_price_scenario_result, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more shop price scenario result.

        Args:
            external_id: External id of the shop price scenario result to delete.
            space: The space where all the shop price scenario result are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete shop_price_scenario_result by id:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> client.shop_price_scenario_result.delete("my_shop_price_scenario_result")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.shop_price_scenario_result.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> SHOPPriceScenarioResult | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> SHOPPriceScenarioResultList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> SHOPPriceScenarioResult | SHOPPriceScenarioResultList | None:
        """Retrieve one or more shop price scenario results by id(s).

        Args:
            external_id: External id or list of external ids of the shop price scenario results.
            space: The space where all the shop price scenario results are located.

        Returns:
            The requested shop price scenario results.

        Examples:

            Retrieve shop_price_scenario_result by id:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> shop_price_scenario_result = client.shop_price_scenario_result.retrieve("my_shop_price_scenario_result")

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
        property: SHOPPriceScenarioResultFields | Sequence[SHOPPriceScenarioResultFields] | None = None,
        group_by: None = None,
        price_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: SHOPPriceScenarioResultFields | Sequence[SHOPPriceScenarioResultFields] | None = None,
        group_by: SHOPPriceScenarioResultFields | Sequence[SHOPPriceScenarioResultFields] = None,
        price_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: SHOPPriceScenarioResultFields | Sequence[SHOPPriceScenarioResultFields] | None = None,
        group_by: SHOPPriceScenarioResultFields | Sequence[SHOPPriceScenarioResultFields] | None = None,
        price_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across shop price scenario results

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            price_scenario: The price scenario to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop price scenario results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count shop price scenario results in space `my_space`:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> result = client.shop_price_scenario_result.aggregate("count", space="my_space")

        """

        filter_ = _create_shop_price_scenario_result_filter(
            self._view_id,
            price_scenario,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _SHOPPRICESCENARIORESULT_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: SHOPPriceScenarioResultFields,
        interval: float,
        price_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for shop price scenario results

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            price_scenario: The price scenario to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop price scenario results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_shop_price_scenario_result_filter(
            self._view_id,
            price_scenario,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _SHOPPRICESCENARIORESULT_PROPERTIES_BY_FIELD,
            None,
            None,
            limit,
            filter_,
        )

    def list(
        self,
        price_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> SHOPPriceScenarioResultList:
        """List/filter shop price scenario results

        Args:
            price_scenario: The price scenario to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop price scenario results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested shop price scenario results

        Examples:

            List shop price scenario results and limit to 5:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> shop_price_scenario_results = client.shop_price_scenario_result.list(limit=5)

        """
        filter_ = _create_shop_price_scenario_result_filter(
            self._view_id,
            price_scenario,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
