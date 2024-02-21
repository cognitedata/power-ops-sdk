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
    SHOPPriceScenario,
    SHOPPriceScenarioWrite,
    SHOPPriceScenarioFields,
    SHOPPriceScenarioList,
    SHOPPriceScenarioWriteList,
    SHOPPriceScenarioTextFields,
)
from cognite.powerops.client._generated.day_ahead_bid.data_classes._shop_price_scenario import (
    _SHOPPRICESCENARIO_PROPERTIES_BY_FIELD,
    _create_shop_price_scenario_filter,
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
from .shop_price_scenario_price import SHOPPriceScenarioPriceAPI
from .shop_price_scenario_query import SHOPPriceScenarioQueryAPI


class SHOPPriceScenarioAPI(NodeAPI[SHOPPriceScenario, SHOPPriceScenarioWrite, SHOPPriceScenarioList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[SHOPPriceScenario]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=SHOPPriceScenario,
            class_list=SHOPPriceScenarioList,
            class_write_list=SHOPPriceScenarioWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.price = SHOPPriceScenarioPriceAPI(client, view_id)

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> SHOPPriceScenarioQueryAPI[SHOPPriceScenarioList]:
        """Query starting at shop price scenarios.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop price scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for shop price scenarios.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_shop_price_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(SHOPPriceScenarioList)
        return SHOPPriceScenarioQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        shop_price_scenario: SHOPPriceScenarioWrite | Sequence[SHOPPriceScenarioWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) shop price scenarios.

        Args:
            shop_price_scenario: Shop price scenario or sequence of shop price scenarios to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new shop_price_scenario:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> from cognite.powerops.client._generated.day_ahead_bid.data_classes import SHOPPriceScenarioWrite
                >>> client = DayAheadBidAPI()
                >>> shop_price_scenario = SHOPPriceScenarioWrite(external_id="my_shop_price_scenario", ...)
                >>> result = client.shop_price_scenario.apply(shop_price_scenario)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.shop_price_scenario.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(shop_price_scenario, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more shop price scenario.

        Args:
            external_id: External id of the shop price scenario to delete.
            space: The space where all the shop price scenario are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete shop_price_scenario by id:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> client.shop_price_scenario.delete("my_shop_price_scenario")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.shop_price_scenario.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> SHOPPriceScenario | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> SHOPPriceScenarioList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> SHOPPriceScenario | SHOPPriceScenarioList | None:
        """Retrieve one or more shop price scenarios by id(s).

        Args:
            external_id: External id or list of external ids of the shop price scenarios.
            space: The space where all the shop price scenarios are located.

        Returns:
            The requested shop price scenarios.

        Examples:

            Retrieve shop_price_scenario by id:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> shop_price_scenario = client.shop_price_scenario.retrieve("my_shop_price_scenario")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: SHOPPriceScenarioTextFields | Sequence[SHOPPriceScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> SHOPPriceScenarioList:
        """Search shop price scenarios

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop price scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results shop price scenarios matching the query.

        Examples:

           Search for 'my_shop_price_scenario' in all text properties:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> shop_price_scenarios = client.shop_price_scenario.search('my_shop_price_scenario')

        """
        filter_ = _create_shop_price_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _SHOPPRICESCENARIO_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: SHOPPriceScenarioFields | Sequence[SHOPPriceScenarioFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: SHOPPriceScenarioTextFields | Sequence[SHOPPriceScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        property: SHOPPriceScenarioFields | Sequence[SHOPPriceScenarioFields] | None = None,
        group_by: SHOPPriceScenarioFields | Sequence[SHOPPriceScenarioFields] = None,
        query: str | None = None,
        search_properties: SHOPPriceScenarioTextFields | Sequence[SHOPPriceScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        property: SHOPPriceScenarioFields | Sequence[SHOPPriceScenarioFields] | None = None,
        group_by: SHOPPriceScenarioFields | Sequence[SHOPPriceScenarioFields] | None = None,
        query: str | None = None,
        search_property: SHOPPriceScenarioTextFields | Sequence[SHOPPriceScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across shop price scenarios

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop price scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count shop price scenarios in space `my_space`:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> result = client.shop_price_scenario.aggregate("count", space="my_space")

        """

        filter_ = _create_shop_price_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _SHOPPRICESCENARIO_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: SHOPPriceScenarioFields,
        interval: float,
        query: str | None = None,
        search_property: SHOPPriceScenarioTextFields | Sequence[SHOPPriceScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for shop price scenarios

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop price scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_shop_price_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _SHOPPRICESCENARIO_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> SHOPPriceScenarioList:
        """List/filter shop price scenarios

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop price scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested shop price scenarios

        Examples:

            List shop price scenarios and limit to 5:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> shop_price_scenarios = client.shop_price_scenario.list(limit=5)

        """
        filter_ = _create_shop_price_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
