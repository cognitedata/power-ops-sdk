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
    ShopOutputTimeSeriesDefinition,
    ShopOutputTimeSeriesDefinitionWrite,
    ShopOutputTimeSeriesDefinitionFields,
    ShopOutputTimeSeriesDefinitionList,
    ShopOutputTimeSeriesDefinitionWriteList,
    ShopOutputTimeSeriesDefinitionTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._shop_output_time_series_definition import (
    _SHOPOUTPUTTIMESERIESDEFINITION_PROPERTIES_BY_FIELD,
    _create_shop_output_time_series_definition_filter,
)
from ._core import DEFAULT_LIMIT_READ, DEFAULT_QUERY_LIMIT, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .shop_output_time_series_definition_query import ShopOutputTimeSeriesDefinitionQueryAPI


class ShopOutputTimeSeriesDefinitionAPI(NodeAPI[ShopOutputTimeSeriesDefinition, ShopOutputTimeSeriesDefinitionWrite, ShopOutputTimeSeriesDefinitionList, ShopOutputTimeSeriesDefinitionWriteList]):
    _view_id = dm.ViewId("power_ops_core", "ShopOutputTimeSeriesDefinition", "1")
    _properties_by_field = _SHOPOUTPUTTIMESERIESDEFINITION_PROPERTIES_BY_FIELD
    _class_type = ShopOutputTimeSeriesDefinition
    _class_list = ShopOutputTimeSeriesDefinitionList
    _class_write_list = ShopOutputTimeSeriesDefinitionWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)


    def __call__(
            self,
            name: str | list[str] | None = None,
            name_prefix: str | None = None,
            object_type: str | list[str] | None = None,
            object_type_prefix: str | None = None,
            object_name: str | list[str] | None = None,
            object_name_prefix: str | None = None,
            attribute_name: str | list[str] | None = None,
            attribute_name_prefix: str | None = None,
            unit: str | list[str] | None = None,
            unit_prefix: str | None = None,
            is_step: bool | None = None,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit: int = DEFAULT_QUERY_LIMIT,
            filter: dm.Filter | None = None,
    ) -> ShopOutputTimeSeriesDefinitionQueryAPI[ShopOutputTimeSeriesDefinitionList]:
        """Query starting at shop output time series definitions.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            object_type: The object type to filter on.
            object_type_prefix: The prefix of the object type to filter on.
            object_name: The object name to filter on.
            object_name_prefix: The prefix of the object name to filter on.
            attribute_name: The attribute name to filter on.
            attribute_name_prefix: The prefix of the attribute name to filter on.
            unit: The unit to filter on.
            unit_prefix: The prefix of the unit to filter on.
            is_step: The is step to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop output time series definitions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for shop output time series definitions.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_shop_output_time_series_definition_filter(
            self._view_id,
            name,
            name_prefix,
            object_type,
            object_type_prefix,
            object_name,
            object_name_prefix,
            attribute_name,
            attribute_name_prefix,
            unit,
            unit_prefix,
            is_step,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(ShopOutputTimeSeriesDefinitionList)
        return ShopOutputTimeSeriesDefinitionQueryAPI(self._client, builder, filter_, limit)


    def apply(
        self,
        shop_output_time_series_definition: ShopOutputTimeSeriesDefinitionWrite | Sequence[ShopOutputTimeSeriesDefinitionWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) shop output time series definitions.

        Args:
            shop_output_time_series_definition: Shop output time series definition or sequence of shop output time series definitions to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new shop_output_time_series_definition:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import ShopOutputTimeSeriesDefinitionWrite
                >>> client = PowerOpsModelsV1Client()
                >>> shop_output_time_series_definition = ShopOutputTimeSeriesDefinitionWrite(external_id="my_shop_output_time_series_definition", ...)
                >>> result = client.shop_output_time_series_definition.apply(shop_output_time_series_definition)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.shop_output_time_series_definition.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(shop_output_time_series_definition, replace, write_none)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> dm.InstancesDeleteResult:
        """Delete one or more shop output time series definition.

        Args:
            external_id: External id of the shop output time series definition to delete.
            space: The space where all the shop output time series definition are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete shop_output_time_series_definition by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.shop_output_time_series_definition.delete("my_shop_output_time_series_definition")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.shop_output_time_series_definition.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> ShopOutputTimeSeriesDefinition | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> ShopOutputTimeSeriesDefinitionList:
        ...

    def retrieve(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> ShopOutputTimeSeriesDefinition | ShopOutputTimeSeriesDefinitionList | None:
        """Retrieve one or more shop output time series definitions by id(s).

        Args:
            external_id: External id or list of external ids of the shop output time series definitions.
            space: The space where all the shop output time series definitions are located.

        Returns:
            The requested shop output time series definitions.

        Examples:

            Retrieve shop_output_time_series_definition by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_output_time_series_definition = client.shop_output_time_series_definition.retrieve("my_shop_output_time_series_definition")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: ShopOutputTimeSeriesDefinitionTextFields | SequenceNotStr[ShopOutputTimeSeriesDefinitionTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
        unit: str | list[str] | None = None,
        unit_prefix: str | None = None,
        is_step: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ShopOutputTimeSeriesDefinitionFields | SequenceNotStr[ShopOutputTimeSeriesDefinitionFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ShopOutputTimeSeriesDefinitionList:
        """Search shop output time series definitions

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            object_type: The object type to filter on.
            object_type_prefix: The prefix of the object type to filter on.
            object_name: The object name to filter on.
            object_name_prefix: The prefix of the object name to filter on.
            attribute_name: The attribute name to filter on.
            attribute_name_prefix: The prefix of the attribute name to filter on.
            unit: The unit to filter on.
            unit_prefix: The prefix of the unit to filter on.
            is_step: The is step to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop output time series definitions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results shop output time series definitions matching the query.

        Examples:

           Search for 'my_shop_output_time_series_definition' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_output_time_series_definitions = client.shop_output_time_series_definition.search('my_shop_output_time_series_definition')

        """
        filter_ = _create_shop_output_time_series_definition_filter(
            self._view_id,
            name,
            name_prefix,
            object_type,
            object_type_prefix,
            object_name,
            object_name_prefix,
            attribute_name,
            attribute_name_prefix,
            unit,
            unit_prefix,
            is_step,
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
        property: ShopOutputTimeSeriesDefinitionFields | SequenceNotStr[ShopOutputTimeSeriesDefinitionFields] | None = None,
        query: str | None = None,
        search_property: ShopOutputTimeSeriesDefinitionTextFields | SequenceNotStr[ShopOutputTimeSeriesDefinitionTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
        unit: str | list[str] | None = None,
        unit_prefix: str | None = None,
        is_step: bool | None = None,
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
        property: ShopOutputTimeSeriesDefinitionFields | SequenceNotStr[ShopOutputTimeSeriesDefinitionFields] | None = None,
        query: str | None = None,
        search_property: ShopOutputTimeSeriesDefinitionTextFields | SequenceNotStr[ShopOutputTimeSeriesDefinitionTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
        unit: str | list[str] | None = None,
        unit_prefix: str | None = None,
        is_step: bool | None = None,
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
        group_by: ShopOutputTimeSeriesDefinitionFields | SequenceNotStr[ShopOutputTimeSeriesDefinitionFields],
        property: ShopOutputTimeSeriesDefinitionFields | SequenceNotStr[ShopOutputTimeSeriesDefinitionFields] | None = None,
        query: str | None = None,
        search_property: ShopOutputTimeSeriesDefinitionTextFields | SequenceNotStr[ShopOutputTimeSeriesDefinitionTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
        unit: str | list[str] | None = None,
        unit_prefix: str | None = None,
        is_step: bool | None = None,
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
        group_by: ShopOutputTimeSeriesDefinitionFields | SequenceNotStr[ShopOutputTimeSeriesDefinitionFields] | None = None,
        property: ShopOutputTimeSeriesDefinitionFields | SequenceNotStr[ShopOutputTimeSeriesDefinitionFields] | None = None,
        query: str | None = None,
        search_property: ShopOutputTimeSeriesDefinitionTextFields | SequenceNotStr[ShopOutputTimeSeriesDefinitionTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
        unit: str | list[str] | None = None,
        unit_prefix: str | None = None,
        is_step: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across shop output time series definitions

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            object_type: The object type to filter on.
            object_type_prefix: The prefix of the object type to filter on.
            object_name: The object name to filter on.
            object_name_prefix: The prefix of the object name to filter on.
            attribute_name: The attribute name to filter on.
            attribute_name_prefix: The prefix of the attribute name to filter on.
            unit: The unit to filter on.
            unit_prefix: The prefix of the unit to filter on.
            is_step: The is step to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop output time series definitions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count shop output time series definitions in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.shop_output_time_series_definition.aggregate("count", space="my_space")

        """

        filter_ = _create_shop_output_time_series_definition_filter(
            self._view_id,
            name,
            name_prefix,
            object_type,
            object_type_prefix,
            object_name,
            object_name_prefix,
            attribute_name,
            attribute_name_prefix,
            unit,
            unit_prefix,
            is_step,
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
        property: ShopOutputTimeSeriesDefinitionFields,
        interval: float,
        query: str | None = None,
        search_property: ShopOutputTimeSeriesDefinitionTextFields | SequenceNotStr[ShopOutputTimeSeriesDefinitionTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
        unit: str | list[str] | None = None,
        unit_prefix: str | None = None,
        is_step: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for shop output time series definitions

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            object_type: The object type to filter on.
            object_type_prefix: The prefix of the object type to filter on.
            object_name: The object name to filter on.
            object_name_prefix: The prefix of the object name to filter on.
            attribute_name: The attribute name to filter on.
            attribute_name_prefix: The prefix of the attribute name to filter on.
            unit: The unit to filter on.
            unit_prefix: The prefix of the unit to filter on.
            is_step: The is step to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop output time series definitions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_shop_output_time_series_definition_filter(
            self._view_id,
            name,
            name_prefix,
            object_type,
            object_type_prefix,
            object_name,
            object_name_prefix,
            attribute_name,
            attribute_name_prefix,
            unit,
            unit_prefix,
            is_step,
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
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
        unit: str | list[str] | None = None,
        unit_prefix: str | None = None,
        is_step: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ShopOutputTimeSeriesDefinitionFields | Sequence[ShopOutputTimeSeriesDefinitionFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ShopOutputTimeSeriesDefinitionList:
        """List/filter shop output time series definitions

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            object_type: The object type to filter on.
            object_type_prefix: The prefix of the object type to filter on.
            object_name: The object name to filter on.
            object_name_prefix: The prefix of the object name to filter on.
            attribute_name: The attribute name to filter on.
            attribute_name_prefix: The prefix of the attribute name to filter on.
            unit: The unit to filter on.
            unit_prefix: The prefix of the unit to filter on.
            is_step: The is step to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop output time series definitions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested shop output time series definitions

        Examples:

            List shop output time series definitions and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_output_time_series_definitions = client.shop_output_time_series_definition.list(limit=5)

        """
        filter_ = _create_shop_output_time_series_definition_filter(
            self._view_id,
            name,
            name_prefix,
            object_type,
            object_type_prefix,
            object_name,
            object_name_prefix,
            attribute_name,
            attribute_name_prefix,
            unit,
            unit_prefix,
            is_step,
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
