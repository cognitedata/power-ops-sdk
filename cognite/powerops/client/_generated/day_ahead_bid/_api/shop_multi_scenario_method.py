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
    SHOPMultiScenarioMethod,
    SHOPMultiScenarioMethodWrite,
    SHOPMultiScenarioMethodFields,
    SHOPMultiScenarioMethodList,
    SHOPMultiScenarioMethodWriteList,
    SHOPMultiScenarioMethodTextFields,
)
from cognite.powerops.client._generated.day_ahead_bid.data_classes._shop_multi_scenario_method import (
    _SHOPMULTISCENARIOMETHOD_PROPERTIES_BY_FIELD,
    _create_shop_multi_scenario_method_filter,
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
from .shop_multi_scenario_method_price_scenarios import SHOPMultiScenarioMethodPriceScenariosAPI
from .shop_multi_scenario_method_query import SHOPMultiScenarioMethodQueryAPI


class SHOPMultiScenarioMethodAPI(
    NodeAPI[SHOPMultiScenarioMethod, SHOPMultiScenarioMethodWrite, SHOPMultiScenarioMethodList]
):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[SHOPMultiScenarioMethod]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=SHOPMultiScenarioMethod,
            class_list=SHOPMultiScenarioMethodList,
            class_write_list=SHOPMultiScenarioMethodWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.price_scenarios_edge = SHOPMultiScenarioMethodPriceScenariosAPI(client)

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> SHOPMultiScenarioMethodQueryAPI[SHOPMultiScenarioMethodList]:
        """Query starting at shop multi scenario methods.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop multi scenario methods to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for shop multi scenario methods.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_shop_multi_scenario_method_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(SHOPMultiScenarioMethodList)
        return SHOPMultiScenarioMethodQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        shop_multi_scenario_method: SHOPMultiScenarioMethodWrite | Sequence[SHOPMultiScenarioMethodWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) shop multi scenario methods.

        Note: This method iterates through all nodes and timeseries linked to shop_multi_scenario_method and creates them including the edges
        between the nodes. For example, if any of `price_scenarios` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            shop_multi_scenario_method: Shop multi scenario method or sequence of shop multi scenario methods to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new shop_multi_scenario_method:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> from cognite.powerops.client._generated.day_ahead_bid.data_classes import SHOPMultiScenarioMethodWrite
                >>> client = DayAheadBidAPI()
                >>> shop_multi_scenario_method = SHOPMultiScenarioMethodWrite(external_id="my_shop_multi_scenario_method", ...)
                >>> result = client.shop_multi_scenario_method.apply(shop_multi_scenario_method)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.shop_multi_scenario_method.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(shop_multi_scenario_method, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more shop multi scenario method.

        Args:
            external_id: External id of the shop multi scenario method to delete.
            space: The space where all the shop multi scenario method are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete shop_multi_scenario_method by id:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> client.shop_multi_scenario_method.delete("my_shop_multi_scenario_method")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.shop_multi_scenario_method.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> SHOPMultiScenarioMethod | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> SHOPMultiScenarioMethodList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> SHOPMultiScenarioMethod | SHOPMultiScenarioMethodList | None:
        """Retrieve one or more shop multi scenario methods by id(s).

        Args:
            external_id: External id or list of external ids of the shop multi scenario methods.
            space: The space where all the shop multi scenario methods are located.

        Returns:
            The requested shop multi scenario methods.

        Examples:

            Retrieve shop_multi_scenario_method by id:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> shop_multi_scenario_method = client.shop_multi_scenario_method.retrieve("my_shop_multi_scenario_method")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.price_scenarios_edge,
                    "price_scenarios",
                    dm.DirectRelationReference("power-ops-types", "PriceScenario"),
                    "outwards",
                    dm.ViewId("power-ops-day-ahead-bid", "SHOPPriceScenario", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: SHOPMultiScenarioMethodTextFields | Sequence[SHOPMultiScenarioMethodTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> SHOPMultiScenarioMethodList:
        """Search shop multi scenario methods

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop multi scenario methods to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results shop multi scenario methods matching the query.

        Examples:

           Search for 'my_shop_multi_scenario_method' in all text properties:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> shop_multi_scenario_methods = client.shop_multi_scenario_method.search('my_shop_multi_scenario_method')

        """
        filter_ = _create_shop_multi_scenario_method_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _SHOPMULTISCENARIOMETHOD_PROPERTIES_BY_FIELD, properties, filter_, limit
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
        property: SHOPMultiScenarioMethodFields | Sequence[SHOPMultiScenarioMethodFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: (
            SHOPMultiScenarioMethodTextFields | Sequence[SHOPMultiScenarioMethodTextFields] | None
        ) = None,
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
        property: SHOPMultiScenarioMethodFields | Sequence[SHOPMultiScenarioMethodFields] | None = None,
        group_by: SHOPMultiScenarioMethodFields | Sequence[SHOPMultiScenarioMethodFields] = None,
        query: str | None = None,
        search_properties: (
            SHOPMultiScenarioMethodTextFields | Sequence[SHOPMultiScenarioMethodTextFields] | None
        ) = None,
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
        property: SHOPMultiScenarioMethodFields | Sequence[SHOPMultiScenarioMethodFields] | None = None,
        group_by: SHOPMultiScenarioMethodFields | Sequence[SHOPMultiScenarioMethodFields] | None = None,
        query: str | None = None,
        search_property: SHOPMultiScenarioMethodTextFields | Sequence[SHOPMultiScenarioMethodTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across shop multi scenario methods

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
            limit: Maximum number of shop multi scenario methods to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count shop multi scenario methods in space `my_space`:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> result = client.shop_multi_scenario_method.aggregate("count", space="my_space")

        """

        filter_ = _create_shop_multi_scenario_method_filter(
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
            _SHOPMULTISCENARIOMETHOD_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: SHOPMultiScenarioMethodFields,
        interval: float,
        query: str | None = None,
        search_property: SHOPMultiScenarioMethodTextFields | Sequence[SHOPMultiScenarioMethodTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for shop multi scenario methods

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop multi scenario methods to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_shop_multi_scenario_method_filter(
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
            _SHOPMULTISCENARIOMETHOD_PROPERTIES_BY_FIELD,
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
        retrieve_edges: bool = True,
    ) -> SHOPMultiScenarioMethodList:
        """List/filter shop multi scenario methods

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop multi scenario methods to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `price_scenarios` external ids for the shop multi scenario methods. Defaults to True.

        Returns:
            List of requested shop multi scenario methods

        Examples:

            List shop multi scenario methods and limit to 5:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> shop_multi_scenario_methods = client.shop_multi_scenario_method.list(limit=5)

        """
        filter_ = _create_shop_multi_scenario_method_filter(
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
            retrieve_edges=retrieve_edges,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.price_scenarios_edge,
                    "price_scenarios",
                    dm.DirectRelationReference("power-ops-types", "PriceScenario"),
                    "outwards",
                    dm.ViewId("power-ops-day-ahead-bid", "SHOPPriceScenario", "1"),
                ),
            ],
        )
