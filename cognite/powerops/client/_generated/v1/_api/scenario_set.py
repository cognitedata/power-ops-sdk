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
    ScenarioSet,
    ScenarioSetWrite,
    ScenarioSetFields,
    ScenarioSetList,
    ScenarioSetWriteList,
    ScenarioSetTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._scenario_set import (
    _SCENARIOSET_PROPERTIES_BY_FIELD,
    _create_scenario_set_filter,
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
from .scenario_set_shop_scenarios import ScenarioSetShopScenariosAPI
from .scenario_set_query import ScenarioSetQueryAPI


class ScenarioSetAPI(NodeAPI[ScenarioSet, ScenarioSetWrite, ScenarioSetList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[ScenarioSet]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ScenarioSet,
            class_list=ScenarioSetList,
            class_write_list=ScenarioSetWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.shop_scenarios_edge = ScenarioSetShopScenariosAPI(client)

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        shop_start_specification: str | list[str] | None = None,
        shop_start_specification_prefix: str | None = None,
        shop_end_specification: str | list[str] | None = None,
        shop_end_specification_prefix: str | None = None,
        shop_bid_date_specification: str | list[str] | None = None,
        shop_bid_date_specification_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ScenarioSetQueryAPI[ScenarioSetList]:
        """Query starting at scenario sets.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            shop_start_specification: The shop start specification to filter on.
            shop_start_specification_prefix: The prefix of the shop start specification to filter on.
            shop_end_specification: The shop end specification to filter on.
            shop_end_specification_prefix: The prefix of the shop end specification to filter on.
            shop_bid_date_specification: The shop bid date specification to filter on.
            shop_bid_date_specification_prefix: The prefix of the shop bid date specification to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenario sets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for scenario sets.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_scenario_set_filter(
            self._view_id,
            name,
            name_prefix,
            shop_start_specification,
            shop_start_specification_prefix,
            shop_end_specification,
            shop_end_specification_prefix,
            shop_bid_date_specification,
            shop_bid_date_specification_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(ScenarioSetList)
        return ScenarioSetQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        scenario_set: ScenarioSetWrite | Sequence[ScenarioSetWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) scenario sets.

        Note: This method iterates through all nodes and timeseries linked to scenario_set and creates them including the edges
        between the nodes. For example, if any of `shop_scenarios` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            scenario_set: Scenario set or sequence of scenario sets to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new scenario_set:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import ScenarioSetWrite
                >>> client = PowerOpsModelsV1Client()
                >>> scenario_set = ScenarioSetWrite(external_id="my_scenario_set", ...)
                >>> result = client.scenario_set.apply(scenario_set)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.scenario_set.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(scenario_set, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more scenario set.

        Args:
            external_id: External id of the scenario set to delete.
            space: The space where all the scenario set are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete scenario_set by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.scenario_set.delete("my_scenario_set")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.scenario_set.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> ScenarioSet | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> ScenarioSetList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ScenarioSet | ScenarioSetList | None:
        """Retrieve one or more scenario sets by id(s).

        Args:
            external_id: External id or list of external ids of the scenario sets.
            space: The space where all the scenario sets are located.

        Returns:
            The requested scenario sets.

        Examples:

            Retrieve scenario_set by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> scenario_set = client.scenario_set.retrieve("my_scenario_set")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.shop_scenarios_edge,
                    "shop_scenarios",
                    dm.DirectRelationReference("sp_powerops_types_temp", "ScenarioSet.scenarios"),
                    "outwards",
                    dm.ViewId("sp_powerops_models_temp", "Scenario", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: ScenarioSetTextFields | Sequence[ScenarioSetTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        shop_start_specification: str | list[str] | None = None,
        shop_start_specification_prefix: str | None = None,
        shop_end_specification: str | list[str] | None = None,
        shop_end_specification_prefix: str | None = None,
        shop_bid_date_specification: str | list[str] | None = None,
        shop_bid_date_specification_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ScenarioSetList:
        """Search scenario sets

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            shop_start_specification: The shop start specification to filter on.
            shop_start_specification_prefix: The prefix of the shop start specification to filter on.
            shop_end_specification: The shop end specification to filter on.
            shop_end_specification_prefix: The prefix of the shop end specification to filter on.
            shop_bid_date_specification: The shop bid date specification to filter on.
            shop_bid_date_specification_prefix: The prefix of the shop bid date specification to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenario sets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results scenario sets matching the query.

        Examples:

           Search for 'my_scenario_set' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> scenario_sets = client.scenario_set.search('my_scenario_set')

        """
        filter_ = _create_scenario_set_filter(
            self._view_id,
            name,
            name_prefix,
            shop_start_specification,
            shop_start_specification_prefix,
            shop_end_specification,
            shop_end_specification_prefix,
            shop_bid_date_specification,
            shop_bid_date_specification_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _SCENARIOSET_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: ScenarioSetFields | Sequence[ScenarioSetFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: ScenarioSetTextFields | Sequence[ScenarioSetTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        shop_start_specification: str | list[str] | None = None,
        shop_start_specification_prefix: str | None = None,
        shop_end_specification: str | list[str] | None = None,
        shop_end_specification_prefix: str | None = None,
        shop_bid_date_specification: str | list[str] | None = None,
        shop_bid_date_specification_prefix: str | None = None,
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
        property: ScenarioSetFields | Sequence[ScenarioSetFields] | None = None,
        group_by: ScenarioSetFields | Sequence[ScenarioSetFields] = None,
        query: str | None = None,
        search_properties: ScenarioSetTextFields | Sequence[ScenarioSetTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        shop_start_specification: str | list[str] | None = None,
        shop_start_specification_prefix: str | None = None,
        shop_end_specification: str | list[str] | None = None,
        shop_end_specification_prefix: str | None = None,
        shop_bid_date_specification: str | list[str] | None = None,
        shop_bid_date_specification_prefix: str | None = None,
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
        property: ScenarioSetFields | Sequence[ScenarioSetFields] | None = None,
        group_by: ScenarioSetFields | Sequence[ScenarioSetFields] | None = None,
        query: str | None = None,
        search_property: ScenarioSetTextFields | Sequence[ScenarioSetTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        shop_start_specification: str | list[str] | None = None,
        shop_start_specification_prefix: str | None = None,
        shop_end_specification: str | list[str] | None = None,
        shop_end_specification_prefix: str | None = None,
        shop_bid_date_specification: str | list[str] | None = None,
        shop_bid_date_specification_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across scenario sets

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            shop_start_specification: The shop start specification to filter on.
            shop_start_specification_prefix: The prefix of the shop start specification to filter on.
            shop_end_specification: The shop end specification to filter on.
            shop_end_specification_prefix: The prefix of the shop end specification to filter on.
            shop_bid_date_specification: The shop bid date specification to filter on.
            shop_bid_date_specification_prefix: The prefix of the shop bid date specification to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenario sets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count scenario sets in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.scenario_set.aggregate("count", space="my_space")

        """

        filter_ = _create_scenario_set_filter(
            self._view_id,
            name,
            name_prefix,
            shop_start_specification,
            shop_start_specification_prefix,
            shop_end_specification,
            shop_end_specification_prefix,
            shop_bid_date_specification,
            shop_bid_date_specification_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _SCENARIOSET_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ScenarioSetFields,
        interval: float,
        query: str | None = None,
        search_property: ScenarioSetTextFields | Sequence[ScenarioSetTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        shop_start_specification: str | list[str] | None = None,
        shop_start_specification_prefix: str | None = None,
        shop_end_specification: str | list[str] | None = None,
        shop_end_specification_prefix: str | None = None,
        shop_bid_date_specification: str | list[str] | None = None,
        shop_bid_date_specification_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for scenario sets

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            shop_start_specification: The shop start specification to filter on.
            shop_start_specification_prefix: The prefix of the shop start specification to filter on.
            shop_end_specification: The shop end specification to filter on.
            shop_end_specification_prefix: The prefix of the shop end specification to filter on.
            shop_bid_date_specification: The shop bid date specification to filter on.
            shop_bid_date_specification_prefix: The prefix of the shop bid date specification to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenario sets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_scenario_set_filter(
            self._view_id,
            name,
            name_prefix,
            shop_start_specification,
            shop_start_specification_prefix,
            shop_end_specification,
            shop_end_specification_prefix,
            shop_bid_date_specification,
            shop_bid_date_specification_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _SCENARIOSET_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        shop_start_specification: str | list[str] | None = None,
        shop_start_specification_prefix: str | None = None,
        shop_end_specification: str | list[str] | None = None,
        shop_end_specification_prefix: str | None = None,
        shop_bid_date_specification: str | list[str] | None = None,
        shop_bid_date_specification_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> ScenarioSetList:
        """List/filter scenario sets

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            shop_start_specification: The shop start specification to filter on.
            shop_start_specification_prefix: The prefix of the shop start specification to filter on.
            shop_end_specification: The shop end specification to filter on.
            shop_end_specification_prefix: The prefix of the shop end specification to filter on.
            shop_bid_date_specification: The shop bid date specification to filter on.
            shop_bid_date_specification_prefix: The prefix of the shop bid date specification to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenario sets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `shop_scenarios` external ids for the scenario sets. Defaults to True.

        Returns:
            List of requested scenario sets

        Examples:

            List scenario sets and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> scenario_sets = client.scenario_set.list(limit=5)

        """
        filter_ = _create_scenario_set_filter(
            self._view_id,
            name,
            name_prefix,
            shop_start_specification,
            shop_start_specification_prefix,
            shop_end_specification,
            shop_end_specification_prefix,
            shop_bid_date_specification,
            shop_bid_date_specification_prefix,
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
                    self.shop_scenarios_edge,
                    "shop_scenarios",
                    dm.DirectRelationReference("sp_powerops_types_temp", "ScenarioSet.scenarios"),
                    "outwards",
                    dm.ViewId("sp_powerops_models_temp", "Scenario", "1"),
                ),
            ],
        )
