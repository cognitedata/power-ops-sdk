from __future__ import annotations

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
    BidMethodSHOPMultiScenario,
    BidMethodSHOPMultiScenarioWrite,
    BidMethodSHOPMultiScenarioFields,
    BidMethodSHOPMultiScenarioList,
    BidMethodSHOPMultiScenarioWriteList,
    BidMethodSHOPMultiScenarioTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._bid_method_shop_multi_scenario import (
    _BIDMETHODSHOPMULTISCENARIO_PROPERTIES_BY_FIELD,
    _create_bid_method_shop_multi_scenario_filter,
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
from .bid_method_shop_multi_scenario_price_scenarios import BidMethodSHOPMultiScenarioPriceScenariosAPI
from .bid_method_shop_multi_scenario_query import BidMethodSHOPMultiScenarioQueryAPI


class BidMethodSHOPMultiScenarioAPI(
    NodeAPI[BidMethodSHOPMultiScenario, BidMethodSHOPMultiScenarioWrite, BidMethodSHOPMultiScenarioList]
):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[BidMethodSHOPMultiScenario]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BidMethodSHOPMultiScenario,
            class_list=BidMethodSHOPMultiScenarioList,
            class_write_list=BidMethodSHOPMultiScenarioWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.price_scenarios_edge = BidMethodSHOPMultiScenarioPriceScenariosAPI(client)

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        main_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> BidMethodSHOPMultiScenarioQueryAPI[BidMethodSHOPMultiScenarioList]:
        """Query starting at bid method shop multi scenarios.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            main_scenario: The main scenario to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid method shop multi scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for bid method shop multi scenarios.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_bid_method_shop_multi_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            main_scenario,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(BidMethodSHOPMultiScenarioList)
        return BidMethodSHOPMultiScenarioQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        bid_method_shop_multi_scenario: BidMethodSHOPMultiScenarioWrite | Sequence[BidMethodSHOPMultiScenarioWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) bid method shop multi scenarios.

        Note: This method iterates through all nodes and timeseries linked to bid_method_shop_multi_scenario and creates them including the edges
        between the nodes. For example, if any of `price_scenarios` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            bid_method_shop_multi_scenario: Bid method shop multi scenario or sequence of bid method shop multi scenarios to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new bid_method_shop_multi_scenario:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import BidMethodSHOPMultiScenarioWrite
                >>> client = PowerOpsModelsV1Client()
                >>> bid_method_shop_multi_scenario = BidMethodSHOPMultiScenarioWrite(external_id="my_bid_method_shop_multi_scenario", ...)
                >>> result = client.bid_method_shop_multi_scenario.apply(bid_method_shop_multi_scenario)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.bid_method_shop_multi_scenario.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(bid_method_shop_multi_scenario, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more bid method shop multi scenario.

        Args:
            external_id: External id of the bid method shop multi scenario to delete.
            space: The space where all the bid method shop multi scenario are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete bid_method_shop_multi_scenario by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.bid_method_shop_multi_scenario.delete("my_bid_method_shop_multi_scenario")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.bid_method_shop_multi_scenario.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> BidMethodSHOPMultiScenario | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> BidMethodSHOPMultiScenarioList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> BidMethodSHOPMultiScenario | BidMethodSHOPMultiScenarioList | None:
        """Retrieve one or more bid method shop multi scenarios by id(s).

        Args:
            external_id: External id or list of external ids of the bid method shop multi scenarios.
            space: The space where all the bid method shop multi scenarios are located.

        Returns:
            The requested bid method shop multi scenarios.

        Examples:

            Retrieve bid_method_shop_multi_scenario by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_method_shop_multi_scenario = client.bid_method_shop_multi_scenario.retrieve("my_bid_method_shop_multi_scenario")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.price_scenarios_edge,
                    "price_scenarios",
                    dm.DirectRelationReference("sp_powerops_types", "BidMethodDayahead.priceScenarios"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "PriceScenario", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: BidMethodSHOPMultiScenarioTextFields | Sequence[BidMethodSHOPMultiScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        main_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidMethodSHOPMultiScenarioList:
        """Search bid method shop multi scenarios

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            main_scenario: The main scenario to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid method shop multi scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results bid method shop multi scenarios matching the query.

        Examples:

           Search for 'my_bid_method_shop_multi_scenario' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_method_shop_multi_scenarios = client.bid_method_shop_multi_scenario.search('my_bid_method_shop_multi_scenario')

        """
        filter_ = _create_bid_method_shop_multi_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            main_scenario,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _BIDMETHODSHOPMULTISCENARIO_PROPERTIES_BY_FIELD, properties, filter_, limit
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
        property: BidMethodSHOPMultiScenarioFields | Sequence[BidMethodSHOPMultiScenarioFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: (
            BidMethodSHOPMultiScenarioTextFields | Sequence[BidMethodSHOPMultiScenarioTextFields] | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        main_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: BidMethodSHOPMultiScenarioFields | Sequence[BidMethodSHOPMultiScenarioFields] | None = None,
        group_by: BidMethodSHOPMultiScenarioFields | Sequence[BidMethodSHOPMultiScenarioFields] = None,
        query: str | None = None,
        search_properties: (
            BidMethodSHOPMultiScenarioTextFields | Sequence[BidMethodSHOPMultiScenarioTextFields] | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        main_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: BidMethodSHOPMultiScenarioFields | Sequence[BidMethodSHOPMultiScenarioFields] | None = None,
        group_by: BidMethodSHOPMultiScenarioFields | Sequence[BidMethodSHOPMultiScenarioFields] | None = None,
        query: str | None = None,
        search_property: (
            BidMethodSHOPMultiScenarioTextFields | Sequence[BidMethodSHOPMultiScenarioTextFields] | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        main_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across bid method shop multi scenarios

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            main_scenario: The main scenario to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid method shop multi scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count bid method shop multi scenarios in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.bid_method_shop_multi_scenario.aggregate("count", space="my_space")

        """

        filter_ = _create_bid_method_shop_multi_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            main_scenario,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _BIDMETHODSHOPMULTISCENARIO_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: BidMethodSHOPMultiScenarioFields,
        interval: float,
        query: str | None = None,
        search_property: (
            BidMethodSHOPMultiScenarioTextFields | Sequence[BidMethodSHOPMultiScenarioTextFields] | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        main_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for bid method shop multi scenarios

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            main_scenario: The main scenario to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid method shop multi scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_bid_method_shop_multi_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            main_scenario,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _BIDMETHODSHOPMULTISCENARIO_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        main_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> BidMethodSHOPMultiScenarioList:
        """List/filter bid method shop multi scenarios

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            main_scenario: The main scenario to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid method shop multi scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `price_scenarios` external ids for the bid method shop multi scenarios. Defaults to True.

        Returns:
            List of requested bid method shop multi scenarios

        Examples:

            List bid method shop multi scenarios and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_method_shop_multi_scenarios = client.bid_method_shop_multi_scenario.list(limit=5)

        """
        filter_ = _create_bid_method_shop_multi_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            main_scenario,
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
                    dm.DirectRelationReference("sp_powerops_types", "BidMethodDayahead.priceScenarios"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "PriceScenario", "1"),
                ),
            ],
        )
