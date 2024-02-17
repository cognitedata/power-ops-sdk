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
    TotalBidMatrixCalculationInput,
    TotalBidMatrixCalculationInputWrite,
    TotalBidMatrixCalculationInputFields,
    TotalBidMatrixCalculationInputList,
    TotalBidMatrixCalculationInputWriteList,
    TotalBidMatrixCalculationInputTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._total_bid_matrix_calculation_input import (
    _TOTALBIDMATRIXCALCULATIONINPUT_PROPERTIES_BY_FIELD,
    _create_total_bid_matrix_calculation_input_filter,
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
from .total_bid_matrix_calculation_input_partial_bid_matrices import TotalBidMatrixCalculationInputPartialBidMatricesAPI
from .total_bid_matrix_calculation_input_query import TotalBidMatrixCalculationInputQueryAPI


class TotalBidMatrixCalculationInputAPI(
    NodeAPI[TotalBidMatrixCalculationInput, TotalBidMatrixCalculationInputWrite, TotalBidMatrixCalculationInputList]
):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[TotalBidMatrixCalculationInput]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=TotalBidMatrixCalculationInput,
            class_list=TotalBidMatrixCalculationInputList,
            class_write_list=TotalBidMatrixCalculationInputWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.partial_bid_matrices_edge = TotalBidMatrixCalculationInputPartialBidMatricesAPI(client)

    def __call__(
        self,
        process_id: str | list[str] | None = None,
        process_id_prefix: str | None = None,
        min_process_step: int | None = None,
        max_process_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> TotalBidMatrixCalculationInputQueryAPI[TotalBidMatrixCalculationInputList]:
        """Query starting at total bid matrix calculation inputs.

        Args:
            process_id: The process id to filter on.
            process_id_prefix: The prefix of the process id to filter on.
            min_process_step: The minimum value of the process step to filter on.
            max_process_step: The maximum value of the process step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of total bid matrix calculation inputs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for total bid matrix calculation inputs.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_total_bid_matrix_calculation_input_filter(
            self._view_id,
            process_id,
            process_id_prefix,
            min_process_step,
            max_process_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(TotalBidMatrixCalculationInputList)
        return TotalBidMatrixCalculationInputQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        total_bid_matrix_calculation_input: (
            TotalBidMatrixCalculationInputWrite | Sequence[TotalBidMatrixCalculationInputWrite]
        ),
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) total bid matrix calculation inputs.

        Note: This method iterates through all nodes and timeseries linked to total_bid_matrix_calculation_input and creates them including the edges
        between the nodes. For example, if any of `partial_bid_matrices` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            total_bid_matrix_calculation_input: Total bid matrix calculation input or sequence of total bid matrix calculation inputs to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new total_bid_matrix_calculation_input:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import TotalBidMatrixCalculationInputWrite
                >>> client = PowerOpsModelsV1Client()
                >>> total_bid_matrix_calculation_input = TotalBidMatrixCalculationInputWrite(external_id="my_total_bid_matrix_calculation_input", ...)
                >>> result = client.total_bid_matrix_calculation_input.apply(total_bid_matrix_calculation_input)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.total_bid_matrix_calculation_input.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(total_bid_matrix_calculation_input, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more total bid matrix calculation input.

        Args:
            external_id: External id of the total bid matrix calculation input to delete.
            space: The space where all the total bid matrix calculation input are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete total_bid_matrix_calculation_input by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.total_bid_matrix_calculation_input.delete("my_total_bid_matrix_calculation_input")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.total_bid_matrix_calculation_input.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE
    ) -> TotalBidMatrixCalculationInput | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> TotalBidMatrixCalculationInputList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> TotalBidMatrixCalculationInput | TotalBidMatrixCalculationInputList | None:
        """Retrieve one or more total bid matrix calculation inputs by id(s).

        Args:
            external_id: External id or list of external ids of the total bid matrix calculation inputs.
            space: The space where all the total bid matrix calculation inputs are located.

        Returns:
            The requested total bid matrix calculation inputs.

        Examples:

            Retrieve total_bid_matrix_calculation_input by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> total_bid_matrix_calculation_input = client.total_bid_matrix_calculation_input.retrieve("my_total_bid_matrix_calculation_input")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.partial_bid_matrices_edge,
                    "partial_bid_matrices",
                    dm.DirectRelationReference("sp_powerops_types", "BidMatrix"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "BidMatrix", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: (
            TotalBidMatrixCalculationInputTextFields | Sequence[TotalBidMatrixCalculationInputTextFields] | None
        ) = None,
        process_id: str | list[str] | None = None,
        process_id_prefix: str | None = None,
        min_process_step: int | None = None,
        max_process_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TotalBidMatrixCalculationInputList:
        """Search total bid matrix calculation inputs

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            process_id: The process id to filter on.
            process_id_prefix: The prefix of the process id to filter on.
            min_process_step: The minimum value of the process step to filter on.
            max_process_step: The maximum value of the process step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of total bid matrix calculation inputs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results total bid matrix calculation inputs matching the query.

        Examples:

           Search for 'my_total_bid_matrix_calculation_input' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> total_bid_matrix_calculation_inputs = client.total_bid_matrix_calculation_input.search('my_total_bid_matrix_calculation_input')

        """
        filter_ = _create_total_bid_matrix_calculation_input_filter(
            self._view_id,
            process_id,
            process_id_prefix,
            min_process_step,
            max_process_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _TOTALBIDMATRIXCALCULATIONINPUT_PROPERTIES_BY_FIELD, properties, filter_, limit
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
        property: TotalBidMatrixCalculationInputFields | Sequence[TotalBidMatrixCalculationInputFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: (
            TotalBidMatrixCalculationInputTextFields | Sequence[TotalBidMatrixCalculationInputTextFields] | None
        ) = None,
        process_id: str | list[str] | None = None,
        process_id_prefix: str | None = None,
        min_process_step: int | None = None,
        max_process_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
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
        property: TotalBidMatrixCalculationInputFields | Sequence[TotalBidMatrixCalculationInputFields] | None = None,
        group_by: TotalBidMatrixCalculationInputFields | Sequence[TotalBidMatrixCalculationInputFields] = None,
        query: str | None = None,
        search_properties: (
            TotalBidMatrixCalculationInputTextFields | Sequence[TotalBidMatrixCalculationInputTextFields] | None
        ) = None,
        process_id: str | list[str] | None = None,
        process_id_prefix: str | None = None,
        min_process_step: int | None = None,
        max_process_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
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
        property: TotalBidMatrixCalculationInputFields | Sequence[TotalBidMatrixCalculationInputFields] | None = None,
        group_by: TotalBidMatrixCalculationInputFields | Sequence[TotalBidMatrixCalculationInputFields] | None = None,
        query: str | None = None,
        search_property: (
            TotalBidMatrixCalculationInputTextFields | Sequence[TotalBidMatrixCalculationInputTextFields] | None
        ) = None,
        process_id: str | list[str] | None = None,
        process_id_prefix: str | None = None,
        min_process_step: int | None = None,
        max_process_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across total bid matrix calculation inputs

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            process_id: The process id to filter on.
            process_id_prefix: The prefix of the process id to filter on.
            min_process_step: The minimum value of the process step to filter on.
            max_process_step: The maximum value of the process step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of total bid matrix calculation inputs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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
            process_id,
            process_id_prefix,
            min_process_step,
            max_process_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _TOTALBIDMATRIXCALCULATIONINPUT_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: TotalBidMatrixCalculationInputFields,
        interval: float,
        query: str | None = None,
        search_property: (
            TotalBidMatrixCalculationInputTextFields | Sequence[TotalBidMatrixCalculationInputTextFields] | None
        ) = None,
        process_id: str | list[str] | None = None,
        process_id_prefix: str | None = None,
        min_process_step: int | None = None,
        max_process_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for total bid matrix calculation inputs

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            process_id: The process id to filter on.
            process_id_prefix: The prefix of the process id to filter on.
            min_process_step: The minimum value of the process step to filter on.
            max_process_step: The maximum value of the process step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of total bid matrix calculation inputs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_total_bid_matrix_calculation_input_filter(
            self._view_id,
            process_id,
            process_id_prefix,
            min_process_step,
            max_process_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _TOTALBIDMATRIXCALCULATIONINPUT_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        process_id: str | list[str] | None = None,
        process_id_prefix: str | None = None,
        min_process_step: int | None = None,
        max_process_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> TotalBidMatrixCalculationInputList:
        """List/filter total bid matrix calculation inputs

        Args:
            process_id: The process id to filter on.
            process_id_prefix: The prefix of the process id to filter on.
            min_process_step: The minimum value of the process step to filter on.
            max_process_step: The maximum value of the process step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of total bid matrix calculation inputs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `partial_bid_matrices` external ids for the total bid matrix calculation inputs. Defaults to True.

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
            process_id,
            process_id_prefix,
            min_process_step,
            max_process_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
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
                    self.partial_bid_matrices_edge,
                    "partial_bid_matrices",
                    dm.DirectRelationReference("sp_powerops_types", "BidMatrix"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "BidMatrix", "1"),
                ),
            ],
        )
