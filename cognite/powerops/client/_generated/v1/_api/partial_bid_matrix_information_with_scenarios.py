from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    PartialBidMatrixInformationWithScenarios,
    PartialBidMatrixInformationWithScenariosWrite,
    PartialBidMatrixInformationWithScenariosFields,
    PartialBidMatrixInformationWithScenariosList,
    PartialBidMatrixInformationWithScenariosWriteList,
    PartialBidMatrixInformationWithScenariosTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._partial_bid_matrix_information_with_scenarios import (
    _PARTIALBIDMATRIXINFORMATIONWITHSCENARIOS_PROPERTIES_BY_FIELD,
    _create_partial_bid_matrix_information_with_scenario_filter,
)
from ._core import DEFAULT_LIMIT_READ, DEFAULT_QUERY_LIMIT, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .partial_bid_matrix_information_with_scenarios_alerts import PartialBidMatrixInformationWithScenariosAlertsAPI
from .partial_bid_matrix_information_with_scenarios_underlying_bid_matrices import PartialBidMatrixInformationWithScenariosUnderlyingBidMatricesAPI
from .partial_bid_matrix_information_with_scenarios_multi_scenario_input import PartialBidMatrixInformationWithScenariosMultiScenarioInputAPI
from .partial_bid_matrix_information_with_scenarios_linked_time_series import PartialBidMatrixInformationWithScenariosLinkedTimeSeriesAPI
from .partial_bid_matrix_information_with_scenarios_query import PartialBidMatrixInformationWithScenariosQueryAPI


class PartialBidMatrixInformationWithScenariosAPI(NodeAPI[PartialBidMatrixInformationWithScenarios, PartialBidMatrixInformationWithScenariosWrite, PartialBidMatrixInformationWithScenariosList, PartialBidMatrixInformationWithScenariosWriteList]):
    _view_id = dm.ViewId("power_ops_core", "PartialBidMatrixInformationWithScenarios", "1")
    _properties_by_field = _PARTIALBIDMATRIXINFORMATIONWITHSCENARIOS_PROPERTIES_BY_FIELD
    _class_type = PartialBidMatrixInformationWithScenarios
    _class_list = PartialBidMatrixInformationWithScenariosList
    _class_write_list = PartialBidMatrixInformationWithScenariosWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.alerts_edge = PartialBidMatrixInformationWithScenariosAlertsAPI(client)
        self.underlying_bid_matrices_edge = PartialBidMatrixInformationWithScenariosUnderlyingBidMatricesAPI(client)
        self.multi_scenario_input_edge = PartialBidMatrixInformationWithScenariosMultiScenarioInputAPI(client)
        self.linked_time_series = PartialBidMatrixInformationWithScenariosLinkedTimeSeriesAPI(client, self._view_id)

    def __call__(
            self,
            state: str | list[str] | None = None,
            state_prefix: str | None = None,
            power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
            min_resource_cost: float | None = None,
            max_resource_cost: float | None = None,
            partial_bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit: int = DEFAULT_QUERY_LIMIT,
            filter: dm.Filter | None = None,
    ) -> PartialBidMatrixInformationWithScenariosQueryAPI[PartialBidMatrixInformationWithScenariosList]:
        """Query starting at partial bid matrix information with scenarios.

        Args:
            state: The state to filter on.
            state_prefix: The prefix of the state to filter on.
            power_asset: The power asset to filter on.
            min_resource_cost: The minimum value of the resource cost to filter on.
            max_resource_cost: The maximum value of the resource cost to filter on.
            partial_bid_configuration: The partial bid configuration to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial bid matrix information with scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for partial bid matrix information with scenarios.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_partial_bid_matrix_information_with_scenario_filter(
            self._view_id,
            state,
            state_prefix,
            power_asset,
            min_resource_cost,
            max_resource_cost,
            partial_bid_configuration,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(PartialBidMatrixInformationWithScenariosList)
        return PartialBidMatrixInformationWithScenariosQueryAPI(self._client, builder, filter_, limit)


    def apply(
        self,
        partial_bid_matrix_information_with_scenario: PartialBidMatrixInformationWithScenariosWrite | Sequence[PartialBidMatrixInformationWithScenariosWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) partial bid matrix information with scenarios.

        Note: This method iterates through all nodes and timeseries linked to partial_bid_matrix_information_with_scenario and creates them including the edges
        between the nodes. For example, if any of `alerts`, `underlying_bid_matrices` or `multi_scenario_input` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            partial_bid_matrix_information_with_scenario: Partial bid matrix information with scenario or sequence of partial bid matrix information with scenarios to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new partial_bid_matrix_information_with_scenario:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import PartialBidMatrixInformationWithScenariosWrite
                >>> client = PowerOpsModelsV1Client()
                >>> partial_bid_matrix_information_with_scenario = PartialBidMatrixInformationWithScenariosWrite(external_id="my_partial_bid_matrix_information_with_scenario", ...)
                >>> result = client.partial_bid_matrix_information_with_scenarios.apply(partial_bid_matrix_information_with_scenario)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.partial_bid_matrix_information_with_scenarios.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(partial_bid_matrix_information_with_scenario, replace, write_none)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> dm.InstancesDeleteResult:
        """Delete one or more partial bid matrix information with scenario.

        Args:
            external_id: External id of the partial bid matrix information with scenario to delete.
            space: The space where all the partial bid matrix information with scenario are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete partial_bid_matrix_information_with_scenario by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.partial_bid_matrix_information_with_scenarios.delete("my_partial_bid_matrix_information_with_scenario")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.partial_bid_matrix_information_with_scenarios.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> PartialBidMatrixInformationWithScenarios | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> PartialBidMatrixInformationWithScenariosList:
        ...

    def retrieve(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> PartialBidMatrixInformationWithScenarios | PartialBidMatrixInformationWithScenariosList | None:
        """Retrieve one or more partial bid matrix information with scenarios by id(s).

        Args:
            external_id: External id or list of external ids of the partial bid matrix information with scenarios.
            space: The space where all the partial bid matrix information with scenarios are located.

        Returns:
            The requested partial bid matrix information with scenarios.

        Examples:

            Retrieve partial_bid_matrix_information_with_scenario by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> partial_bid_matrix_information_with_scenario = client.partial_bid_matrix_information_with_scenarios.retrieve("my_partial_bid_matrix_information_with_scenario")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.alerts_edge,
                    "alerts",
                    dm.DirectRelationReference("power_ops_types", "calculationIssue"),
                    "outwards",
                    dm.ViewId("power_ops_core", "Alert", "1"),
                ),
                (
                    self.underlying_bid_matrices_edge,
                    "underlying_bid_matrices",
                    dm.DirectRelationReference("power_ops_types", "intermediateBidMatrix"),
                    "outwards",
                    dm.ViewId("power_ops_core", "BidMatrix", "1"),
                ),
                (
                    self.multi_scenario_input_edge,
                    "multi_scenario_input",
                    dm.DirectRelationReference("power_ops_types", "calculationIssue"),
                    "outwards",
                    dm.ViewId("power_ops_core", "PriceProduction", "1"),
                ),
                                               ]
        )


    def search(
        self,
        query: str,
        properties: PartialBidMatrixInformationWithScenariosTextFields | SequenceNotStr[PartialBidMatrixInformationWithScenariosTextFields] | None = None,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_resource_cost: float | None = None,
        max_resource_cost: float | None = None,
        partial_bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: PartialBidMatrixInformationWithScenariosFields | SequenceNotStr[PartialBidMatrixInformationWithScenariosFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> PartialBidMatrixInformationWithScenariosList:
        """Search partial bid matrix information with scenarios

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            state: The state to filter on.
            state_prefix: The prefix of the state to filter on.
            power_asset: The power asset to filter on.
            min_resource_cost: The minimum value of the resource cost to filter on.
            max_resource_cost: The maximum value of the resource cost to filter on.
            partial_bid_configuration: The partial bid configuration to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial bid matrix information with scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results partial bid matrix information with scenarios matching the query.

        Examples:

           Search for 'my_partial_bid_matrix_information_with_scenario' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> partial_bid_matrix_information_with_scenarios = client.partial_bid_matrix_information_with_scenarios.search('my_partial_bid_matrix_information_with_scenario')

        """
        filter_ = _create_partial_bid_matrix_information_with_scenario_filter(
            self._view_id,
            state,
            state_prefix,
            power_asset,
            min_resource_cost,
            max_resource_cost,
            partial_bid_configuration,
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
        property: PartialBidMatrixInformationWithScenariosFields | SequenceNotStr[PartialBidMatrixInformationWithScenariosFields] | None = None,
        query: str | None = None,
        search_property: PartialBidMatrixInformationWithScenariosTextFields | SequenceNotStr[PartialBidMatrixInformationWithScenariosTextFields] | None = None,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_resource_cost: float | None = None,
        max_resource_cost: float | None = None,
        partial_bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.AggregatedNumberedValue:
        ...

    @overload
    def aggregate(
        self,
        aggregate: SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: None = None,
        property: PartialBidMatrixInformationWithScenariosFields | SequenceNotStr[PartialBidMatrixInformationWithScenariosFields] | None = None,
        query: str | None = None,
        search_property: PartialBidMatrixInformationWithScenariosTextFields | SequenceNotStr[PartialBidMatrixInformationWithScenariosTextFields] | None = None,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_resource_cost: float | None = None,
        max_resource_cost: float | None = None,
        partial_bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: PartialBidMatrixInformationWithScenariosFields | SequenceNotStr[PartialBidMatrixInformationWithScenariosFields],
        property: PartialBidMatrixInformationWithScenariosFields | SequenceNotStr[PartialBidMatrixInformationWithScenariosFields] | None = None,
        query: str | None = None,
        search_property: PartialBidMatrixInformationWithScenariosTextFields | SequenceNotStr[PartialBidMatrixInformationWithScenariosTextFields] | None = None,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_resource_cost: float | None = None,
        max_resource_cost: float | None = None,
        partial_bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: PartialBidMatrixInformationWithScenariosFields | SequenceNotStr[PartialBidMatrixInformationWithScenariosFields] | None = None,
        property: PartialBidMatrixInformationWithScenariosFields | SequenceNotStr[PartialBidMatrixInformationWithScenariosFields] | None = None,
        query: str | None = None,
        search_property: PartialBidMatrixInformationWithScenariosTextFields | SequenceNotStr[PartialBidMatrixInformationWithScenariosTextFields] | None = None,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_resource_cost: float | None = None,
        max_resource_cost: float | None = None,
        partial_bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across partial bid matrix information with scenarios

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            state: The state to filter on.
            state_prefix: The prefix of the state to filter on.
            power_asset: The power asset to filter on.
            min_resource_cost: The minimum value of the resource cost to filter on.
            max_resource_cost: The maximum value of the resource cost to filter on.
            partial_bid_configuration: The partial bid configuration to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial bid matrix information with scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count partial bid matrix information with scenarios in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.partial_bid_matrix_information_with_scenarios.aggregate("count", space="my_space")

        """

        filter_ = _create_partial_bid_matrix_information_with_scenario_filter(
            self._view_id,
            state,
            state_prefix,
            power_asset,
            min_resource_cost,
            max_resource_cost,
            partial_bid_configuration,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            aggregate=aggregate,
            group_by=group_by,  # type: ignore[arg-type]
            properties=property,  # type: ignore[arg-type]
            query=query,
            search_properties=search_property,  # type: ignore[arg-type]
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: PartialBidMatrixInformationWithScenariosFields,
        interval: float,
        query: str | None = None,
        search_property: PartialBidMatrixInformationWithScenariosTextFields | SequenceNotStr[PartialBidMatrixInformationWithScenariosTextFields] | None = None,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_resource_cost: float | None = None,
        max_resource_cost: float | None = None,
        partial_bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for partial bid matrix information with scenarios

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            state: The state to filter on.
            state_prefix: The prefix of the state to filter on.
            power_asset: The power asset to filter on.
            min_resource_cost: The minimum value of the resource cost to filter on.
            max_resource_cost: The maximum value of the resource cost to filter on.
            partial_bid_configuration: The partial bid configuration to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial bid matrix information with scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_partial_bid_matrix_information_with_scenario_filter(
            self._view_id,
            state,
            state_prefix,
            power_asset,
            min_resource_cost,
            max_resource_cost,
            partial_bid_configuration,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            property,
            interval,
            query,
            search_property,  # type: ignore[arg-type]
            limit,
            filter_,
        )


    def list(
        self,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_resource_cost: float | None = None,
        max_resource_cost: float | None = None,
        partial_bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: PartialBidMatrixInformationWithScenariosFields | Sequence[PartialBidMatrixInformationWithScenariosFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_edges: bool = True,
    ) -> PartialBidMatrixInformationWithScenariosList:
        """List/filter partial bid matrix information with scenarios

        Args:
            state: The state to filter on.
            state_prefix: The prefix of the state to filter on.
            power_asset: The power asset to filter on.
            min_resource_cost: The minimum value of the resource cost to filter on.
            max_resource_cost: The maximum value of the resource cost to filter on.
            partial_bid_configuration: The partial bid configuration to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial bid matrix information with scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_edges: Whether to retrieve `alerts`, `underlying_bid_matrices` or `multi_scenario_input` external ids for the partial bid matrix information with scenarios. Defaults to True.

        Returns:
            List of requested partial bid matrix information with scenarios

        Examples:

            List partial bid matrix information with scenarios and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> partial_bid_matrix_information_with_scenarios = client.partial_bid_matrix_information_with_scenarios.list(limit=5)

        """
        filter_ = _create_partial_bid_matrix_information_with_scenario_filter(
            self._view_id,
            state,
            state_prefix,
            power_asset,
            min_resource_cost,
            max_resource_cost,
            partial_bid_configuration,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.alerts_edge,
                    "alerts",
                    dm.DirectRelationReference("power_ops_types", "calculationIssue"),
                    "outwards",
                    dm.ViewId("power_ops_core", "Alert", "1"),
                ),
                (
                    self.underlying_bid_matrices_edge,
                    "underlying_bid_matrices",
                    dm.DirectRelationReference("power_ops_types", "intermediateBidMatrix"),
                    "outwards",
                    dm.ViewId("power_ops_core", "BidMatrix", "1"),
                ),
                (
                    self.multi_scenario_input_edge,
                    "multi_scenario_input",
                    dm.DirectRelationReference("power_ops_types", "calculationIssue"),
                    "outwards",
                    dm.ViewId("power_ops_core", "PriceProduction", "1"),
                ),
                                               ]
        )
