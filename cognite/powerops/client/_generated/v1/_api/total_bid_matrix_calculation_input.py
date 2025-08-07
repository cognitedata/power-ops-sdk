from __future__ import annotations

import datetime
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
from cognite.powerops.client._generated.v1.data_classes._total_bid_matrix_calculation_input import (
    TotalBidMatrixCalculationInputQuery,
    _TOTALBIDMATRIXCALCULATIONINPUT_PROPERTIES_BY_FIELD,
    _create_total_bid_matrix_calculation_input_filter,
)
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    TotalBidMatrixCalculationInput,
    TotalBidMatrixCalculationInputWrite,
    TotalBidMatrixCalculationInputFields,
    TotalBidMatrixCalculationInputList,
    TotalBidMatrixCalculationInputWriteList,
    TotalBidMatrixCalculationInputTextFields,
    BidConfigurationDayAhead,
    BidMatrix,
)
from cognite.powerops.client._generated.v1._api.total_bid_matrix_calculation_input_partial_bid_matrices import TotalBidMatrixCalculationInputPartialBidMatricesAPI


class TotalBidMatrixCalculationInputAPI(NodeAPI[TotalBidMatrixCalculationInput, TotalBidMatrixCalculationInputWrite, TotalBidMatrixCalculationInputList, TotalBidMatrixCalculationInputWriteList]):
    _view_id = dm.ViewId("power_ops_core", "TotalBidMatrixCalculationInput", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _TOTALBIDMATRIXCALCULATIONINPUT_PROPERTIES_BY_FIELD
    _class_type = TotalBidMatrixCalculationInput
    _class_list = TotalBidMatrixCalculationInputList
    _class_write_list = TotalBidMatrixCalculationInputWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.partial_bid_matrices_edge = TotalBidMatrixCalculationInputPartialBidMatricesAPI(client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> TotalBidMatrixCalculationInput | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> TotalBidMatrixCalculationInputList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> TotalBidMatrixCalculationInput | TotalBidMatrixCalculationInputList | None:
        """Retrieve one or more total bid matrix calculation inputs by id(s).

        Args:
            external_id: External id or list of external ids of the total bid matrix calculation inputs.
            space: The space where all the total bid matrix calculation inputs are located.
            retrieve_connections: Whether to retrieve `bid_configuration` and `partial_bid_matrices` for the total bid
            matrix calculation inputs. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will
            only retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            The requested total bid matrix calculation inputs.

        Examples:

            Retrieve total_bid_matrix_calculation_input by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> total_bid_matrix_calculation_input = client.total_bid_matrix_calculation_input.retrieve(
                ...     "my_total_bid_matrix_calculation_input"
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
        properties: TotalBidMatrixCalculationInputTextFields | SequenceNotStr[TotalBidMatrixCalculationInputTextFields] | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_workflow_step: int | None = None,
        max_workflow_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_bid_date: datetime.date | None = None,
        max_bid_date: datetime.date | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: TotalBidMatrixCalculationInputFields | SequenceNotStr[TotalBidMatrixCalculationInputFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> TotalBidMatrixCalculationInputList:
        """Search total bid matrix calculation inputs

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            min_workflow_step: The minimum value of the workflow step to filter on.
            max_workflow_step: The maximum value of the workflow step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            bid_configuration: The bid configuration to filter on.
            min_bid_date: The minimum value of the bid date to filter on.
            max_bid_date: The maximum value of the bid date to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of total bid matrix calculation inputs to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results total bid matrix calculation inputs matching the query.

        Examples:

           Search for 'my_total_bid_matrix_calculation_input' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> total_bid_matrix_calculation_inputs = client.total_bid_matrix_calculation_input.search(
                ...     'my_total_bid_matrix_calculation_input'
                ... )

        """
        filter_ = _create_total_bid_matrix_calculation_input_filter(
            self._view_id,
            workflow_execution_id,
            workflow_execution_id_prefix,
            min_workflow_step,
            max_workflow_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            bid_configuration,
            min_bid_date,
            max_bid_date,
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
        property: TotalBidMatrixCalculationInputFields | SequenceNotStr[TotalBidMatrixCalculationInputFields] | None = None,
        query: str | None = None,
        search_property: TotalBidMatrixCalculationInputTextFields | SequenceNotStr[TotalBidMatrixCalculationInputTextFields] | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_workflow_step: int | None = None,
        max_workflow_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_bid_date: datetime.date | None = None,
        max_bid_date: datetime.date | None = None,
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
        property: TotalBidMatrixCalculationInputFields | SequenceNotStr[TotalBidMatrixCalculationInputFields] | None = None,
        query: str | None = None,
        search_property: TotalBidMatrixCalculationInputTextFields | SequenceNotStr[TotalBidMatrixCalculationInputTextFields] | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_workflow_step: int | None = None,
        max_workflow_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_bid_date: datetime.date | None = None,
        max_bid_date: datetime.date | None = None,
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
        group_by: TotalBidMatrixCalculationInputFields | SequenceNotStr[TotalBidMatrixCalculationInputFields],
        property: TotalBidMatrixCalculationInputFields | SequenceNotStr[TotalBidMatrixCalculationInputFields] | None = None,
        query: str | None = None,
        search_property: TotalBidMatrixCalculationInputTextFields | SequenceNotStr[TotalBidMatrixCalculationInputTextFields] | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_workflow_step: int | None = None,
        max_workflow_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_bid_date: datetime.date | None = None,
        max_bid_date: datetime.date | None = None,
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
        group_by: TotalBidMatrixCalculationInputFields | SequenceNotStr[TotalBidMatrixCalculationInputFields] | None = None,
        property: TotalBidMatrixCalculationInputFields | SequenceNotStr[TotalBidMatrixCalculationInputFields] | None = None,
        query: str | None = None,
        search_property: TotalBidMatrixCalculationInputTextFields | SequenceNotStr[TotalBidMatrixCalculationInputTextFields] | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_workflow_step: int | None = None,
        max_workflow_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_bid_date: datetime.date | None = None,
        max_bid_date: datetime.date | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across total bid matrix calculation inputs

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            min_workflow_step: The minimum value of the workflow step to filter on.
            max_workflow_step: The maximum value of the workflow step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            bid_configuration: The bid configuration to filter on.
            min_bid_date: The minimum value of the bid date to filter on.
            max_bid_date: The maximum value of the bid date to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of total bid matrix calculation inputs to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count total bid matrix calculation inputs in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.total_bid_matrix_calculation_input.aggregate("count", space="my_space")

        """

        filter_ = _create_total_bid_matrix_calculation_input_filter(
            self._view_id,
            workflow_execution_id,
            workflow_execution_id_prefix,
            min_workflow_step,
            max_workflow_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            bid_configuration,
            min_bid_date,
            max_bid_date,
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
        property: TotalBidMatrixCalculationInputFields,
        interval: float,
        query: str | None = None,
        search_property: TotalBidMatrixCalculationInputTextFields | SequenceNotStr[TotalBidMatrixCalculationInputTextFields] | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_workflow_step: int | None = None,
        max_workflow_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_bid_date: datetime.date | None = None,
        max_bid_date: datetime.date | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for total bid matrix calculation inputs

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            min_workflow_step: The minimum value of the workflow step to filter on.
            max_workflow_step: The maximum value of the workflow step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            bid_configuration: The bid configuration to filter on.
            min_bid_date: The minimum value of the bid date to filter on.
            max_bid_date: The maximum value of the bid date to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of total bid matrix calculation inputs to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_total_bid_matrix_calculation_input_filter(
            self._view_id,
            workflow_execution_id,
            workflow_execution_id_prefix,
            min_workflow_step,
            max_workflow_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            bid_configuration,
            min_bid_date,
            max_bid_date,
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

    def select(self) -> TotalBidMatrixCalculationInputQuery:
        """Start selecting from total bid matrix calculation inputs."""
        return TotalBidMatrixCalculationInputQuery(self._client)

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
                    BidMatrix._view_id,
                    "outwards",
                    ViewPropertyId(self._view_id, "partialBidMatrices"),
                    include_end_node=retrieve_connections == "full",
                    has_container_fields=True,
                )
            )
        if retrieve_connections == "full":
            builder.extend(
                factory.from_direct_relation(
                    BidConfigurationDayAhead._view_id,
                    ViewPropertyId(self._view_id, "bidConfiguration"),
                    has_container_fields=True,
                )
            )
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_workflow_step: int | None = None,
        max_workflow_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_bid_date: datetime.date | None = None,
        max_bid_date: datetime.date | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[TotalBidMatrixCalculationInputList]:
        """Iterate over total bid matrix calculation inputs

        Args:
            chunk_size: The number of total bid matrix calculation inputs to return in each iteration. Defaults to 100.
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            min_workflow_step: The minimum value of the workflow step to filter on.
            max_workflow_step: The maximum value of the workflow step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            bid_configuration: The bid configuration to filter on.
            min_bid_date: The minimum value of the bid date to filter on.
            max_bid_date: The maximum value of the bid date to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `bid_configuration` and `partial_bid_matrices` for the total bid
            matrix calculation inputs. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will
            only retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.
            limit: Maximum number of total bid matrix calculation inputs to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of total bid matrix calculation inputs

        Examples:

            Iterate total bid matrix calculation inputs in chunks of 100 up to 2000 items:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for total_bid_matrix_calculation_inputs in client.total_bid_matrix_calculation_input.iterate(chunk_size=100, limit=2000):
                ...     for total_bid_matrix_calculation_input in total_bid_matrix_calculation_inputs:
                ...         print(total_bid_matrix_calculation_input.external_id)

            Iterate total bid matrix calculation inputs in chunks of 100 sorted by external_id in descending order:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for total_bid_matrix_calculation_inputs in client.total_bid_matrix_calculation_input.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for total_bid_matrix_calculation_input in total_bid_matrix_calculation_inputs:
                ...         print(total_bid_matrix_calculation_input.external_id)

            Iterate total bid matrix calculation inputs in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for first_iteration in client.total_bid_matrix_calculation_input.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for total_bid_matrix_calculation_inputs in client.total_bid_matrix_calculation_input.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for total_bid_matrix_calculation_input in total_bid_matrix_calculation_inputs:
                ...         print(total_bid_matrix_calculation_input.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_total_bid_matrix_calculation_input_filter(
            self._view_id,
            workflow_execution_id,
            workflow_execution_id_prefix,
            min_workflow_step,
            max_workflow_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            bid_configuration,
            min_bid_date,
            max_bid_date,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, retrieve_connections, cursors=cursors)

    def list(
        self,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_workflow_step: int | None = None,
        max_workflow_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        min_bid_date: datetime.date | None = None,
        max_bid_date: datetime.date | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: TotalBidMatrixCalculationInputFields | Sequence[TotalBidMatrixCalculationInputFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> TotalBidMatrixCalculationInputList:
        """List/filter total bid matrix calculation inputs

        Args:
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            min_workflow_step: The minimum value of the workflow step to filter on.
            max_workflow_step: The maximum value of the workflow step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            bid_configuration: The bid configuration to filter on.
            min_bid_date: The minimum value of the bid date to filter on.
            max_bid_date: The maximum value of the bid date to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of total bid matrix calculation inputs to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `bid_configuration` and `partial_bid_matrices` for the total bid
            matrix calculation inputs. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will
            only retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested total bid matrix calculation inputs

        Examples:

            List total bid matrix calculation inputs and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> total_bid_matrix_calculation_inputs = client.total_bid_matrix_calculation_input.list(limit=5)

        """
        filter_ = _create_total_bid_matrix_calculation_input_filter(
            self._view_id,
            workflow_execution_id,
            workflow_execution_id_prefix,
            min_workflow_step,
            max_workflow_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            bid_configuration,
            min_bid_date,
            max_bid_date,
            external_id_prefix,
            space,
            filter,
        )
        sort_input =  self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        if retrieve_connections == "skip":
            return self._list(limit=limit,  filter=filter_, sort=sort_input)
        return self._query(filter_, limit, retrieve_connections, sort_input, "list")
