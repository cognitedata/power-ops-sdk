from __future__ import annotations

import warnings
from collections.abc import Iterator, Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite.powerops.client._generated.v1._api._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_CHUNK_SIZE,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryBuildStepFactory,
    QueryBuilder,
    QueryExecutor,
    QueryUnpacker,
    ViewPropertyId,
)
from cognite.powerops.client._generated.v1.data_classes._partial_bid_matrix_information_with_scenarios import (
    PartialBidMatrixInformationWithScenariosQuery,
    _PARTIALBIDMATRIXINFORMATIONWITHSCENARIOS_PROPERTIES_BY_FIELD,
    _create_partial_bid_matrix_information_with_scenario_filter,
)
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    PartialBidMatrixInformationWithScenarios,
    PartialBidMatrixInformationWithScenariosWrite,
    PartialBidMatrixInformationWithScenariosFields,
    PartialBidMatrixInformationWithScenariosList,
    PartialBidMatrixInformationWithScenariosWriteList,
    PartialBidMatrixInformationWithScenariosTextFields,
    Alert,
    BidMatrix,
    PartialBidConfiguration,
    PowerAsset,
    PriceProduction,
)
from cognite.powerops.client._generated.v1._api.partial_bid_matrix_information_with_scenarios_alerts import PartialBidMatrixInformationWithScenariosAlertsAPI
from cognite.powerops.client._generated.v1._api.partial_bid_matrix_information_with_scenarios_underlying_bid_matrices import PartialBidMatrixInformationWithScenariosUnderlyingBidMatricesAPI
from cognite.powerops.client._generated.v1._api.partial_bid_matrix_information_with_scenarios_multi_scenario_input import PartialBidMatrixInformationWithScenariosMultiScenarioInputAPI


class PartialBidMatrixInformationWithScenariosAPI(NodeAPI[PartialBidMatrixInformationWithScenarios, PartialBidMatrixInformationWithScenariosWrite, PartialBidMatrixInformationWithScenariosList, PartialBidMatrixInformationWithScenariosWriteList]):
    _view_id = dm.ViewId("power_ops_core", "PartialBidMatrixInformationWithScenarios", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _PARTIALBIDMATRIXINFORMATIONWITHSCENARIOS_PROPERTIES_BY_FIELD
    _class_type = PartialBidMatrixInformationWithScenarios
    _class_list = PartialBidMatrixInformationWithScenariosList
    _class_write_list = PartialBidMatrixInformationWithScenariosWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.alerts_edge = PartialBidMatrixInformationWithScenariosAlertsAPI(client)
        self.underlying_bid_matrices_edge = PartialBidMatrixInformationWithScenariosUnderlyingBidMatricesAPI(client)
        self.multi_scenario_input_edge = PartialBidMatrixInformationWithScenariosMultiScenarioInputAPI(client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> PartialBidMatrixInformationWithScenarios | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> PartialBidMatrixInformationWithScenariosList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> PartialBidMatrixInformationWithScenarios | PartialBidMatrixInformationWithScenariosList | None:
        """Retrieve one or more partial bid matrix information with scenarios by id(s).

        Args:
            external_id: External id or list of external ids of the partial bid matrix information with scenarios.
            space: The space where all the partial bid matrix information with scenarios are located.
            retrieve_connections: Whether to retrieve `alerts`, `underlying_bid_matrices`, `power_asset`,
            `partial_bid_configuration` and `multi_scenario_input` for the partial bid matrix information with
            scenarios. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the
            identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            The requested partial bid matrix information with scenarios.

        Examples:

            Retrieve partial_bid_matrix_information_with_scenario by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> partial_bid_matrix_information_with_scenario = client.partial_bid_matrix_information_with_scenarios.retrieve(
                ...     "my_partial_bid_matrix_information_with_scenario"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_connections=retrieve_connections,
        )

    def search(
        self,
        query: str,
        properties: PartialBidMatrixInformationWithScenariosTextFields | SequenceNotStr[PartialBidMatrixInformationWithScenariosTextFields] | None = None,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        power_asset: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_resource_cost: float | None = None,
        max_resource_cost: float | None = None,
        partial_bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
            limit: Maximum number of partial bid matrix information with scenarios to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results partial bid matrix information with scenarios matching the query.

        Examples:

           Search for 'my_partial_bid_matrix_information_with_scenario' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> partial_bid_matrix_information_with_scenarios = client.partial_bid_matrix_information_with_scenarios.search(
                ...     'my_partial_bid_matrix_information_with_scenario'
                ... )

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
        power_asset: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_resource_cost: float | None = None,
        max_resource_cost: float | None = None,
        partial_bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.AggregatedNumberedValue: ...

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
        power_asset: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_resource_cost: float | None = None,
        max_resource_cost: float | None = None,
        partial_bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

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
        power_asset: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_resource_cost: float | None = None,
        max_resource_cost: float | None = None,
        partial_bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

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
        power_asset: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_resource_cost: float | None = None,
        max_resource_cost: float | None = None,
        partial_bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
            limit: Maximum number of partial bid matrix information with scenarios to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

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
        power_asset: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_resource_cost: float | None = None,
        max_resource_cost: float | None = None,
        partial_bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
            limit: Maximum number of partial bid matrix information with scenarios to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

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

    def select(self) -> PartialBidMatrixInformationWithScenariosQuery:
        """Start selecting from partial bid matrix information with scenarios."""
        return PartialBidMatrixInformationWithScenariosQuery(self._client)

    def _build(
        self,
        filter_: dm.Filter | None,
        limit: int | None,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
        chunk_size: int | None = None,
    ) -> QueryExecutor:
        builder = QueryBuilder()
        factory = QueryBuildStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(factory.root(
            filter=filter_,
            sort=sort,
            limit=limit,
            max_retrieve_batch_limit=chunk_size,
            has_container_fields=True,
        ))
        if retrieve_connections == "identifier" or retrieve_connections == "full":
            builder.extend(
                factory.from_edge(
                    Alert._view_id,
                    "outwards",
                    ViewPropertyId(self._view_id, "alerts"),
                    include_end_node=retrieve_connections == "full",
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_edge(
                    BidMatrix._view_id,
                    "outwards",
                    ViewPropertyId(self._view_id, "underlyingBidMatrices"),
                    include_end_node=retrieve_connections == "full",
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_edge(
                    PriceProduction._view_id,
                    "outwards",
                    ViewPropertyId(self._view_id, "multiScenarioInput"),
                    include_end_node=retrieve_connections == "full",
                    has_container_fields=True,
                )
            )
        if retrieve_connections == "full":
            builder.extend(
                factory.from_direct_relation(
                    PowerAsset._view_id,
                    ViewPropertyId(self._view_id, "powerAsset"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    PartialBidConfiguration._view_id,
                    ViewPropertyId(self._view_id, "partialBidConfiguration"),
                    has_container_fields=True,
                )
            )
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        power_asset: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_resource_cost: float | None = None,
        max_resource_cost: float | None = None,
        partial_bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[PartialBidMatrixInformationWithScenariosList]:
        """Iterate over partial bid matrix information with scenarios

        Args:
            chunk_size: The number of partial bid matrix information with scenarios to return in each iteration. Defaults to 100.
            state: The state to filter on.
            state_prefix: The prefix of the state to filter on.
            power_asset: The power asset to filter on.
            min_resource_cost: The minimum value of the resource cost to filter on.
            max_resource_cost: The maximum value of the resource cost to filter on.
            partial_bid_configuration: The partial bid configuration to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `alerts`, `underlying_bid_matrices`, `power_asset`,
            `partial_bid_configuration` and `multi_scenario_input` for the partial bid matrix information with
            scenarios. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the
            identifier of the connected items, and 'full' will retrieve the full connected items.
            limit: Maximum number of partial bid matrix information with scenarios to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of partial bid matrix information with scenarios

        Examples:

            Iterate partial bid matrix information with scenarios in chunks of 100 up to 2000 items:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for partial_bid_matrix_information_with_scenarios in client.partial_bid_matrix_information_with_scenarios.iterate(chunk_size=100, limit=2000):
                ...     for partial_bid_matrix_information_with_scenario in partial_bid_matrix_information_with_scenarios:
                ...         print(partial_bid_matrix_information_with_scenario.external_id)

            Iterate partial bid matrix information with scenarios in chunks of 100 sorted by external_id in descending order:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for partial_bid_matrix_information_with_scenarios in client.partial_bid_matrix_information_with_scenarios.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for partial_bid_matrix_information_with_scenario in partial_bid_matrix_information_with_scenarios:
                ...         print(partial_bid_matrix_information_with_scenario.external_id)

            Iterate partial bid matrix information with scenarios in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for first_iteration in client.partial_bid_matrix_information_with_scenarios.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for partial_bid_matrix_information_with_scenarios in client.partial_bid_matrix_information_with_scenarios.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for partial_bid_matrix_information_with_scenario in partial_bid_matrix_information_with_scenarios:
                ...         print(partial_bid_matrix_information_with_scenario.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
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
        yield from self._iterate(chunk_size, filter_, limit, retrieve_connections, cursors=cursors)

    def list(
        self,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        power_asset: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_resource_cost: float | None = None,
        max_resource_cost: float | None = None,
        partial_bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: PartialBidMatrixInformationWithScenariosFields | Sequence[PartialBidMatrixInformationWithScenariosFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
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
            limit: Maximum number of partial bid matrix information with scenarios to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `alerts`, `underlying_bid_matrices`, `power_asset`,
            `partial_bid_configuration` and `multi_scenario_input` for the partial bid matrix information with
            scenarios. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the
            identifier of the connected items, and 'full' will retrieve the full connected items.

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
        sort_input =  self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        if retrieve_connections == "skip":
            return self._list(limit=limit,  filter=filter_, sort=sort_input)
        return self._query(filter_, limit, retrieve_connections, sort_input, "list")
