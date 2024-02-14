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
    ShopPartialBidCalculationOutput,
    ShopPartialBidCalculationOutputWrite,
    ShopPartialBidCalculationOutputFields,
    ShopPartialBidCalculationOutputList,
    ShopPartialBidCalculationOutputWriteList,
    ShopPartialBidCalculationOutputTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._shop_partial_bid_calculation_output import (
    _SHOPPARTIALBIDCALCULATIONOUTPUT_PROPERTIES_BY_FIELD,
    _create_shop_partial_bid_calculation_output_filter,
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
from .shop_partial_bid_calculation_output_alerts import ShopPartialBidCalculationOutputAlertsAPI
from .shop_partial_bid_calculation_output_query import ShopPartialBidCalculationOutputQueryAPI


class ShopPartialBidCalculationOutputAPI(
    NodeAPI[ShopPartialBidCalculationOutput, ShopPartialBidCalculationOutputWrite, ShopPartialBidCalculationOutputList]
):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[ShopPartialBidCalculationOutput]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ShopPartialBidCalculationOutput,
            class_list=ShopPartialBidCalculationOutputList,
            class_write_list=ShopPartialBidCalculationOutputWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.alerts_edge = ShopPartialBidCalculationOutputAlertsAPI(client)

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
        bid_matrix_raw: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        input_: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ShopPartialBidCalculationOutputQueryAPI[ShopPartialBidCalculationOutputList]:
        """Query starting at shop partial bid calculation outputs.

        Args:
            process_id: The process id to filter on.
            process_id_prefix: The prefix of the process id to filter on.
            min_process_step: The minimum value of the process step to filter on.
            max_process_step: The maximum value of the process step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            bid_matrix_raw: The bid matrix raw to filter on.
            input_: The input to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop partial bid calculation outputs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for shop partial bid calculation outputs.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_shop_partial_bid_calculation_output_filter(
            self._view_id,
            process_id,
            process_id_prefix,
            min_process_step,
            max_process_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            bid_matrix_raw,
            input_,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(ShopPartialBidCalculationOutputList)
        return ShopPartialBidCalculationOutputQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        shop_partial_bid_calculation_output: (
            ShopPartialBidCalculationOutputWrite | Sequence[ShopPartialBidCalculationOutputWrite]
        ),
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) shop partial bid calculation outputs.

        Note: This method iterates through all nodes and timeseries linked to shop_partial_bid_calculation_output and creates them including the edges
        between the nodes. For example, if any of `alerts` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            shop_partial_bid_calculation_output: Shop partial bid calculation output or sequence of shop partial bid calculation outputs to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new shop_partial_bid_calculation_output:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import ShopPartialBidCalculationOutputWrite
                >>> client = PowerOpsModelsV1Client()
                >>> shop_partial_bid_calculation_output = ShopPartialBidCalculationOutputWrite(external_id="my_shop_partial_bid_calculation_output", ...)
                >>> result = client.shop_partial_bid_calculation_output.apply(shop_partial_bid_calculation_output)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.shop_partial_bid_calculation_output.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(shop_partial_bid_calculation_output, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more shop partial bid calculation output.

        Args:
            external_id: External id of the shop partial bid calculation output to delete.
            space: The space where all the shop partial bid calculation output are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete shop_partial_bid_calculation_output by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.shop_partial_bid_calculation_output.delete("my_shop_partial_bid_calculation_output")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.shop_partial_bid_calculation_output.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE
    ) -> ShopPartialBidCalculationOutput | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ShopPartialBidCalculationOutputList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ShopPartialBidCalculationOutput | ShopPartialBidCalculationOutputList | None:
        """Retrieve one or more shop partial bid calculation outputs by id(s).

        Args:
            external_id: External id or list of external ids of the shop partial bid calculation outputs.
            space: The space where all the shop partial bid calculation outputs are located.

        Returns:
            The requested shop partial bid calculation outputs.

        Examples:

            Retrieve shop_partial_bid_calculation_output by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_partial_bid_calculation_output = client.shop_partial_bid_calculation_output.retrieve("my_shop_partial_bid_calculation_output")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.alerts_edge,
                    "alerts",
                    dm.DirectRelationReference("sp_powerops_types", "calculationIssue"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "Alert", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: (
            ShopPartialBidCalculationOutputTextFields | Sequence[ShopPartialBidCalculationOutputTextFields] | None
        ) = None,
        process_id: str | list[str] | None = None,
        process_id_prefix: str | None = None,
        min_process_step: int | None = None,
        max_process_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        bid_matrix_raw: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        input_: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ShopPartialBidCalculationOutputList:
        """Search shop partial bid calculation outputs

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
            bid_matrix_raw: The bid matrix raw to filter on.
            input_: The input to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop partial bid calculation outputs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results shop partial bid calculation outputs matching the query.

        Examples:

           Search for 'my_shop_partial_bid_calculation_output' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_partial_bid_calculation_outputs = client.shop_partial_bid_calculation_output.search('my_shop_partial_bid_calculation_output')

        """
        filter_ = _create_shop_partial_bid_calculation_output_filter(
            self._view_id,
            process_id,
            process_id_prefix,
            min_process_step,
            max_process_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            bid_matrix_raw,
            input_,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _SHOPPARTIALBIDCALCULATIONOUTPUT_PROPERTIES_BY_FIELD, properties, filter_, limit
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
        property: ShopPartialBidCalculationOutputFields | Sequence[ShopPartialBidCalculationOutputFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: (
            ShopPartialBidCalculationOutputTextFields | Sequence[ShopPartialBidCalculationOutputTextFields] | None
        ) = None,
        process_id: str | list[str] | None = None,
        process_id_prefix: str | None = None,
        min_process_step: int | None = None,
        max_process_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        bid_matrix_raw: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        input_: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: ShopPartialBidCalculationOutputFields | Sequence[ShopPartialBidCalculationOutputFields] | None = None,
        group_by: ShopPartialBidCalculationOutputFields | Sequence[ShopPartialBidCalculationOutputFields] = None,
        query: str | None = None,
        search_properties: (
            ShopPartialBidCalculationOutputTextFields | Sequence[ShopPartialBidCalculationOutputTextFields] | None
        ) = None,
        process_id: str | list[str] | None = None,
        process_id_prefix: str | None = None,
        min_process_step: int | None = None,
        max_process_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        bid_matrix_raw: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        input_: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: ShopPartialBidCalculationOutputFields | Sequence[ShopPartialBidCalculationOutputFields] | None = None,
        group_by: ShopPartialBidCalculationOutputFields | Sequence[ShopPartialBidCalculationOutputFields] | None = None,
        query: str | None = None,
        search_property: (
            ShopPartialBidCalculationOutputTextFields | Sequence[ShopPartialBidCalculationOutputTextFields] | None
        ) = None,
        process_id: str | list[str] | None = None,
        process_id_prefix: str | None = None,
        min_process_step: int | None = None,
        max_process_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        bid_matrix_raw: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        input_: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across shop partial bid calculation outputs

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
            bid_matrix_raw: The bid matrix raw to filter on.
            input_: The input to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop partial bid calculation outputs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count shop partial bid calculation outputs in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.shop_partial_bid_calculation_output.aggregate("count", space="my_space")

        """

        filter_ = _create_shop_partial_bid_calculation_output_filter(
            self._view_id,
            process_id,
            process_id_prefix,
            min_process_step,
            max_process_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            bid_matrix_raw,
            input_,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _SHOPPARTIALBIDCALCULATIONOUTPUT_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ShopPartialBidCalculationOutputFields,
        interval: float,
        query: str | None = None,
        search_property: (
            ShopPartialBidCalculationOutputTextFields | Sequence[ShopPartialBidCalculationOutputTextFields] | None
        ) = None,
        process_id: str | list[str] | None = None,
        process_id_prefix: str | None = None,
        min_process_step: int | None = None,
        max_process_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        bid_matrix_raw: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        input_: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for shop partial bid calculation outputs

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
            bid_matrix_raw: The bid matrix raw to filter on.
            input_: The input to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop partial bid calculation outputs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_shop_partial_bid_calculation_output_filter(
            self._view_id,
            process_id,
            process_id_prefix,
            min_process_step,
            max_process_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            bid_matrix_raw,
            input_,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _SHOPPARTIALBIDCALCULATIONOUTPUT_PROPERTIES_BY_FIELD,
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
        bid_matrix_raw: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        input_: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> ShopPartialBidCalculationOutputList:
        """List/filter shop partial bid calculation outputs

        Args:
            process_id: The process id to filter on.
            process_id_prefix: The prefix of the process id to filter on.
            min_process_step: The minimum value of the process step to filter on.
            max_process_step: The maximum value of the process step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            bid_matrix_raw: The bid matrix raw to filter on.
            input_: The input to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop partial bid calculation outputs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `alerts` external ids for the shop partial bid calculation outputs. Defaults to True.

        Returns:
            List of requested shop partial bid calculation outputs

        Examples:

            List shop partial bid calculation outputs and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_partial_bid_calculation_outputs = client.shop_partial_bid_calculation_output.list(limit=5)

        """
        filter_ = _create_shop_partial_bid_calculation_output_filter(
            self._view_id,
            process_id,
            process_id_prefix,
            min_process_step,
            max_process_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            bid_matrix_raw,
            input_,
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
                    self.alerts_edge,
                    "alerts",
                    dm.DirectRelationReference("sp_powerops_types", "calculationIssue"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "Alert", "1"),
                ),
            ],
        )
