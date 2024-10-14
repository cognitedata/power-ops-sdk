from __future__ import annotations

import datetime
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
    ShopPreprocessorInput,
    ShopPreprocessorInputWrite,
    ShopPreprocessorInputFields,
    ShopPreprocessorInputList,
    ShopPreprocessorInputWriteList,
    ShopPreprocessorInputTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._shop_preprocessor_input import (
    _SHOPPREPROCESSORINPUT_PROPERTIES_BY_FIELD,
    _create_shop_preprocessor_input_filter,
)
from ._core import DEFAULT_LIMIT_READ, DEFAULT_QUERY_LIMIT, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .shop_preprocessor_input_query import ShopPreprocessorInputQueryAPI


class ShopPreprocessorInputAPI(NodeAPI[ShopPreprocessorInput, ShopPreprocessorInputWrite, ShopPreprocessorInputList, ShopPreprocessorInputWriteList]):
    _view_id = dm.ViewId("power_ops_core", "ShopPreprocessorInput", "1")
    _properties_by_field = _SHOPPREPROCESSORINPUT_PROPERTIES_BY_FIELD
    _class_type = ShopPreprocessorInput
    _class_list = ShopPreprocessorInputList
    _class_write_list = ShopPreprocessorInputWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)


    def __call__(
            self,
            workflow_execution_id: str | list[str] | None = None,
            workflow_execution_id_prefix: str | None = None,
            min_workflow_step: int | None = None,
            max_workflow_step: int | None = None,
            function_name: str | list[str] | None = None,
            function_name_prefix: str | None = None,
            function_call_id: str | list[str] | None = None,
            function_call_id_prefix: str | None = None,
            scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
            min_start_time: datetime.datetime | None = None,
            max_start_time: datetime.datetime | None = None,
            min_end_time: datetime.datetime | None = None,
            max_end_time: datetime.datetime | None = None,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit: int = DEFAULT_QUERY_LIMIT,
            filter: dm.Filter | None = None,
    ) -> ShopPreprocessorInputQueryAPI[ShopPreprocessorInputList]:
        """Query starting at shop preprocessor inputs.

        Args:
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            min_workflow_step: The minimum value of the workflow step to filter on.
            max_workflow_step: The maximum value of the workflow step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            scenario: The scenario to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop preprocessor inputs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for shop preprocessor inputs.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_shop_preprocessor_input_filter(
            self._view_id,
            workflow_execution_id,
            workflow_execution_id_prefix,
            min_workflow_step,
            max_workflow_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            scenario,
            min_start_time,
            max_start_time,
            min_end_time,
            max_end_time,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(ShopPreprocessorInputList)
        return ShopPreprocessorInputQueryAPI(self._client, builder, filter_, limit)


    def apply(
        self,
        shop_preprocessor_input: ShopPreprocessorInputWrite | Sequence[ShopPreprocessorInputWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) shop preprocessor inputs.

        Args:
            shop_preprocessor_input: Shop preprocessor input or sequence of shop preprocessor inputs to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new shop_preprocessor_input:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import ShopPreprocessorInputWrite
                >>> client = PowerOpsModelsV1Client()
                >>> shop_preprocessor_input = ShopPreprocessorInputWrite(external_id="my_shop_preprocessor_input", ...)
                >>> result = client.shop_preprocessor_input.apply(shop_preprocessor_input)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.shop_preprocessor_input.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(shop_preprocessor_input, replace, write_none)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> dm.InstancesDeleteResult:
        """Delete one or more shop preprocessor input.

        Args:
            external_id: External id of the shop preprocessor input to delete.
            space: The space where all the shop preprocessor input are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete shop_preprocessor_input by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.shop_preprocessor_input.delete("my_shop_preprocessor_input")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.shop_preprocessor_input.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> ShopPreprocessorInput | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> ShopPreprocessorInputList:
        ...

    def retrieve(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> ShopPreprocessorInput | ShopPreprocessorInputList | None:
        """Retrieve one or more shop preprocessor inputs by id(s).

        Args:
            external_id: External id or list of external ids of the shop preprocessor inputs.
            space: The space where all the shop preprocessor inputs are located.

        Returns:
            The requested shop preprocessor inputs.

        Examples:

            Retrieve shop_preprocessor_input by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_preprocessor_input = client.shop_preprocessor_input.retrieve("my_shop_preprocessor_input")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: ShopPreprocessorInputTextFields | SequenceNotStr[ShopPreprocessorInputTextFields] | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_workflow_step: int | None = None,
        max_workflow_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ShopPreprocessorInputFields | SequenceNotStr[ShopPreprocessorInputFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ShopPreprocessorInputList:
        """Search shop preprocessor inputs

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
            scenario: The scenario to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop preprocessor inputs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results shop preprocessor inputs matching the query.

        Examples:

           Search for 'my_shop_preprocessor_input' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_preprocessor_inputs = client.shop_preprocessor_input.search('my_shop_preprocessor_input')

        """
        filter_ = _create_shop_preprocessor_input_filter(
            self._view_id,
            workflow_execution_id,
            workflow_execution_id_prefix,
            min_workflow_step,
            max_workflow_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            scenario,
            min_start_time,
            max_start_time,
            min_end_time,
            max_end_time,
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
        property: ShopPreprocessorInputFields | SequenceNotStr[ShopPreprocessorInputFields] | None = None,
        query: str | None = None,
        search_property: ShopPreprocessorInputTextFields | SequenceNotStr[ShopPreprocessorInputTextFields] | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_workflow_step: int | None = None,
        max_workflow_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
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
        property: ShopPreprocessorInputFields | SequenceNotStr[ShopPreprocessorInputFields] | None = None,
        query: str | None = None,
        search_property: ShopPreprocessorInputTextFields | SequenceNotStr[ShopPreprocessorInputTextFields] | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_workflow_step: int | None = None,
        max_workflow_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
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
        group_by: ShopPreprocessorInputFields | SequenceNotStr[ShopPreprocessorInputFields],
        property: ShopPreprocessorInputFields | SequenceNotStr[ShopPreprocessorInputFields] | None = None,
        query: str | None = None,
        search_property: ShopPreprocessorInputTextFields | SequenceNotStr[ShopPreprocessorInputTextFields] | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_workflow_step: int | None = None,
        max_workflow_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
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
        group_by: ShopPreprocessorInputFields | SequenceNotStr[ShopPreprocessorInputFields] | None = None,
        property: ShopPreprocessorInputFields | SequenceNotStr[ShopPreprocessorInputFields] | None = None,
        query: str | None = None,
        search_property: ShopPreprocessorInputTextFields | SequenceNotStr[ShopPreprocessorInputTextFields] | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_workflow_step: int | None = None,
        max_workflow_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across shop preprocessor inputs

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
            scenario: The scenario to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop preprocessor inputs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count shop preprocessor inputs in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.shop_preprocessor_input.aggregate("count", space="my_space")

        """

        filter_ = _create_shop_preprocessor_input_filter(
            self._view_id,
            workflow_execution_id,
            workflow_execution_id_prefix,
            min_workflow_step,
            max_workflow_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            scenario,
            min_start_time,
            max_start_time,
            min_end_time,
            max_end_time,
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
        property: ShopPreprocessorInputFields,
        interval: float,
        query: str | None = None,
        search_property: ShopPreprocessorInputTextFields | SequenceNotStr[ShopPreprocessorInputTextFields] | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_workflow_step: int | None = None,
        max_workflow_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for shop preprocessor inputs

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
            scenario: The scenario to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop preprocessor inputs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_shop_preprocessor_input_filter(
            self._view_id,
            workflow_execution_id,
            workflow_execution_id_prefix,
            min_workflow_step,
            max_workflow_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            scenario,
            min_start_time,
            max_start_time,
            min_end_time,
            max_end_time,
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
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_workflow_step: int | None = None,
        max_workflow_step: int | None = None,
        function_name: str | list[str] | None = None,
        function_name_prefix: str | None = None,
        function_call_id: str | list[str] | None = None,
        function_call_id_prefix: str | None = None,
        scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_start_time: datetime.datetime | None = None,
        max_start_time: datetime.datetime | None = None,
        min_end_time: datetime.datetime | None = None,
        max_end_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ShopPreprocessorInputFields | Sequence[ShopPreprocessorInputFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ShopPreprocessorInputList:
        """List/filter shop preprocessor inputs

        Args:
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            min_workflow_step: The minimum value of the workflow step to filter on.
            max_workflow_step: The maximum value of the workflow step to filter on.
            function_name: The function name to filter on.
            function_name_prefix: The prefix of the function name to filter on.
            function_call_id: The function call id to filter on.
            function_call_id_prefix: The prefix of the function call id to filter on.
            scenario: The scenario to filter on.
            min_start_time: The minimum value of the start time to filter on.
            max_start_time: The maximum value of the start time to filter on.
            min_end_time: The minimum value of the end time to filter on.
            max_end_time: The maximum value of the end time to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop preprocessor inputs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested shop preprocessor inputs

        Examples:

            List shop preprocessor inputs and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_preprocessor_inputs = client.shop_preprocessor_input.list(limit=5)

        """
        filter_ = _create_shop_preprocessor_input_filter(
            self._view_id,
            workflow_execution_id,
            workflow_execution_id_prefix,
            min_workflow_step,
            max_workflow_step,
            function_name,
            function_name_prefix,
            function_call_id,
            function_call_id_prefix,
            scenario,
            min_start_time,
            max_start_time,
            min_end_time,
            max_end_time,
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
        )
