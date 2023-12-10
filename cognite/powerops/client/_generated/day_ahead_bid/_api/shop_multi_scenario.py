from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.day_ahead_bid.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.day_ahead_bid.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    SHOPMultiScenario,
    SHOPMultiScenarioApply,
    SHOPMultiScenarioFields,
    SHOPMultiScenarioList,
    SHOPMultiScenarioApplyList,
    SHOPMultiScenarioTextFields,
)
from cognite.powerops.client._generated.day_ahead_bid.data_classes._shop_multi_scenario import (
    _SHOPMULTISCENARIO_PROPERTIES_BY_FIELD,
    _create_shop_multi_scenario_filter,
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
from .shop_multi_scenario_price_scenarios import SHOPMultiScenarioPriceScenariosAPI
from .shop_multi_scenario_query import SHOPMultiScenarioQueryAPI


class SHOPMultiScenarioAPI(NodeAPI[SHOPMultiScenario, SHOPMultiScenarioApply, SHOPMultiScenarioList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[SHOPMultiScenarioApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=SHOPMultiScenario,
            class_apply_type=SHOPMultiScenarioApply,
            class_list=SHOPMultiScenarioList,
            class_apply_list=SHOPMultiScenarioApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.price_scenarios = SHOPMultiScenarioPriceScenariosAPI(client, view_id)

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> SHOPMultiScenarioQueryAPI[SHOPMultiScenarioList]:
        """Query starting at shop multi scenarios.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop multi scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for shop multi scenarios.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_shop_multi_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(SHOPMultiScenarioList)
        return SHOPMultiScenarioQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self, shop_multi_scenario: SHOPMultiScenarioApply | Sequence[SHOPMultiScenarioApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) shop multi scenarios.

        Args:
            shop_multi_scenario: Shop multi scenario or sequence of shop multi scenarios to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new shop_multi_scenario:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> from cognite.powerops.client._generated.day_ahead_bid.data_classes import SHOPMultiScenarioApply
                >>> client = DayAheadBidAPI()
                >>> shop_multi_scenario = SHOPMultiScenarioApply(external_id="my_shop_multi_scenario", ...)
                >>> result = client.shop_multi_scenario.apply(shop_multi_scenario)

        """
        return self._apply(shop_multi_scenario, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more shop multi scenario.

        Args:
            external_id: External id of the shop multi scenario to delete.
            space: The space where all the shop multi scenario are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete shop_multi_scenario by id:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> client.shop_multi_scenario.delete("my_shop_multi_scenario")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> SHOPMultiScenario | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> SHOPMultiScenarioList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> SHOPMultiScenario | SHOPMultiScenarioList | None:
        """Retrieve one or more shop multi scenarios by id(s).

        Args:
            external_id: External id or list of external ids of the shop multi scenarios.
            space: The space where all the shop multi scenarios are located.

        Returns:
            The requested shop multi scenarios.

        Examples:

            Retrieve shop_multi_scenario by id:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> shop_multi_scenario = client.shop_multi_scenario.retrieve("my_shop_multi_scenario")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: SHOPMultiScenarioTextFields | Sequence[SHOPMultiScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> SHOPMultiScenarioList:
        """Search shop multi scenarios

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop multi scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results shop multi scenarios matching the query.

        Examples:

           Search for 'my_shop_multi_scenario' in all text properties:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> shop_multi_scenarios = client.shop_multi_scenario.search('my_shop_multi_scenario')

        """
        filter_ = _create_shop_multi_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _SHOPMULTISCENARIO_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: SHOPMultiScenarioFields | Sequence[SHOPMultiScenarioFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: SHOPMultiScenarioTextFields | Sequence[SHOPMultiScenarioTextFields] | None = None,
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
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: SHOPMultiScenarioFields | Sequence[SHOPMultiScenarioFields] | None = None,
        group_by: SHOPMultiScenarioFields | Sequence[SHOPMultiScenarioFields] = None,
        query: str | None = None,
        search_properties: SHOPMultiScenarioTextFields | Sequence[SHOPMultiScenarioTextFields] | None = None,
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
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: SHOPMultiScenarioFields | Sequence[SHOPMultiScenarioFields] | None = None,
        group_by: SHOPMultiScenarioFields | Sequence[SHOPMultiScenarioFields] | None = None,
        query: str | None = None,
        search_property: SHOPMultiScenarioTextFields | Sequence[SHOPMultiScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across shop multi scenarios

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
            limit: Maximum number of shop multi scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count shop multi scenarios in space `my_space`:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> result = client.shop_multi_scenario.aggregate("count", space="my_space")

        """

        filter_ = _create_shop_multi_scenario_filter(
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
            _SHOPMULTISCENARIO_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: SHOPMultiScenarioFields,
        interval: float,
        query: str | None = None,
        search_property: SHOPMultiScenarioTextFields | Sequence[SHOPMultiScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for shop multi scenarios

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop multi scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_shop_multi_scenario_filter(
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
            _SHOPMULTISCENARIO_PROPERTIES_BY_FIELD,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> SHOPMultiScenarioList:
        """List/filter shop multi scenarios

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop multi scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested shop multi scenarios

        Examples:

            List shop multi scenarios and limit to 5:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> shop_multi_scenarios = client.shop_multi_scenario.list(limit=5)

        """
        filter_ = _create_shop_multi_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
