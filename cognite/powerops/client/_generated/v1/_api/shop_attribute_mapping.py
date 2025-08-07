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
from cognite.powerops.client._generated.v1.data_classes._shop_attribute_mapping import (
    ShopAttributeMappingQuery,
    _SHOPATTRIBUTEMAPPING_PROPERTIES_BY_FIELD,
    _create_shop_attribute_mapping_filter,
)
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    ShopAttributeMapping,
    ShopAttributeMappingWrite,
    ShopAttributeMappingFields,
    ShopAttributeMappingList,
    ShopAttributeMappingWriteList,
    ShopAttributeMappingTextFields,
)


class ShopAttributeMappingAPI(NodeAPI[ShopAttributeMapping, ShopAttributeMappingWrite, ShopAttributeMappingList, ShopAttributeMappingWriteList]):
    _view_id = dm.ViewId("power_ops_core", "ShopAttributeMapping", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _SHOPATTRIBUTEMAPPING_PROPERTIES_BY_FIELD
    _class_type = ShopAttributeMapping
    _class_list = ShopAttributeMappingList
    _class_write_list = ShopAttributeMappingWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)


    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> ShopAttributeMapping | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> ShopAttributeMappingList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> ShopAttributeMapping | ShopAttributeMappingList | None:
        """Retrieve one or more shop attribute mappings by id(s).

        Args:
            external_id: External id or list of external ids of the shop attribute mappings.
            space: The space where all the shop attribute mappings are located.

        Returns:
            The requested shop attribute mappings.

        Examples:

            Retrieve shop_attribute_mapping by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_attribute_mapping = client.shop_attribute_mapping.retrieve(
                ...     "my_shop_attribute_mapping"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
        )

    def search(
        self,
        query: str,
        properties: ShopAttributeMappingTextFields | SequenceNotStr[ShopAttributeMappingTextFields] | None = None,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ShopAttributeMappingFields | SequenceNotStr[ShopAttributeMappingFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ShopAttributeMappingList:
        """Search shop attribute mappings

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            object_type: The object type to filter on.
            object_type_prefix: The prefix of the object type to filter on.
            object_name: The object name to filter on.
            object_name_prefix: The prefix of the object name to filter on.
            attribute_name: The attribute name to filter on.
            attribute_name_prefix: The prefix of the attribute name to filter on.
            retrieve: The retrieve to filter on.
            retrieve_prefix: The prefix of the retrieve to filter on.
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop attribute mappings to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results shop attribute mappings matching the query.

        Examples:

           Search for 'my_shop_attribute_mapping' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_attribute_mappings = client.shop_attribute_mapping.search(
                ...     'my_shop_attribute_mapping'
                ... )

        """
        filter_ = _create_shop_attribute_mapping_filter(
            self._view_id,
            object_type,
            object_type_prefix,
            object_name,
            object_name_prefix,
            attribute_name,
            attribute_name_prefix,
            retrieve,
            retrieve_prefix,
            aggregation,
            aggregation_prefix,
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
        property: ShopAttributeMappingFields | SequenceNotStr[ShopAttributeMappingFields] | None = None,
        query: str | None = None,
        search_property: ShopAttributeMappingTextFields | SequenceNotStr[ShopAttributeMappingTextFields] | None = None,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
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
        property: ShopAttributeMappingFields | SequenceNotStr[ShopAttributeMappingFields] | None = None,
        query: str | None = None,
        search_property: ShopAttributeMappingTextFields | SequenceNotStr[ShopAttributeMappingTextFields] | None = None,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
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
        group_by: ShopAttributeMappingFields | SequenceNotStr[ShopAttributeMappingFields],
        property: ShopAttributeMappingFields | SequenceNotStr[ShopAttributeMappingFields] | None = None,
        query: str | None = None,
        search_property: ShopAttributeMappingTextFields | SequenceNotStr[ShopAttributeMappingTextFields] | None = None,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
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
        group_by: ShopAttributeMappingFields | SequenceNotStr[ShopAttributeMappingFields] | None = None,
        property: ShopAttributeMappingFields | SequenceNotStr[ShopAttributeMappingFields] | None = None,
        query: str | None = None,
        search_property: ShopAttributeMappingTextFields | SequenceNotStr[ShopAttributeMappingTextFields] | None = None,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across shop attribute mappings

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            object_type: The object type to filter on.
            object_type_prefix: The prefix of the object type to filter on.
            object_name: The object name to filter on.
            object_name_prefix: The prefix of the object name to filter on.
            attribute_name: The attribute name to filter on.
            attribute_name_prefix: The prefix of the attribute name to filter on.
            retrieve: The retrieve to filter on.
            retrieve_prefix: The prefix of the retrieve to filter on.
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop attribute mappings to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count shop attribute mappings in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.shop_attribute_mapping.aggregate("count", space="my_space")

        """

        filter_ = _create_shop_attribute_mapping_filter(
            self._view_id,
            object_type,
            object_type_prefix,
            object_name,
            object_name_prefix,
            attribute_name,
            attribute_name_prefix,
            retrieve,
            retrieve_prefix,
            aggregation,
            aggregation_prefix,
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
        property: ShopAttributeMappingFields,
        interval: float,
        query: str | None = None,
        search_property: ShopAttributeMappingTextFields | SequenceNotStr[ShopAttributeMappingTextFields] | None = None,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for shop attribute mappings

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            object_type: The object type to filter on.
            object_type_prefix: The prefix of the object type to filter on.
            object_name: The object name to filter on.
            object_name_prefix: The prefix of the object name to filter on.
            attribute_name: The attribute name to filter on.
            attribute_name_prefix: The prefix of the attribute name to filter on.
            retrieve: The retrieve to filter on.
            retrieve_prefix: The prefix of the retrieve to filter on.
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop attribute mappings to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_shop_attribute_mapping_filter(
            self._view_id,
            object_type,
            object_type_prefix,
            object_name,
            object_name_prefix,
            attribute_name,
            attribute_name_prefix,
            retrieve,
            retrieve_prefix,
            aggregation,
            aggregation_prefix,
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

    def select(self) -> ShopAttributeMappingQuery:
        """Start selecting from shop attribute mappings."""
        return ShopAttributeMappingQuery(self._client)

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
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[ShopAttributeMappingList]:
        """Iterate over shop attribute mappings

        Args:
            chunk_size: The number of shop attribute mappings to return in each iteration. Defaults to 100.
            object_type: The object type to filter on.
            object_type_prefix: The prefix of the object type to filter on.
            object_name: The object name to filter on.
            object_name_prefix: The prefix of the object name to filter on.
            attribute_name: The attribute name to filter on.
            attribute_name_prefix: The prefix of the attribute name to filter on.
            retrieve: The retrieve to filter on.
            retrieve_prefix: The prefix of the retrieve to filter on.
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of shop attribute mappings to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of shop attribute mappings

        Examples:

            Iterate shop attribute mappings in chunks of 100 up to 2000 items:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for shop_attribute_mappings in client.shop_attribute_mapping.iterate(chunk_size=100, limit=2000):
                ...     for shop_attribute_mapping in shop_attribute_mappings:
                ...         print(shop_attribute_mapping.external_id)

            Iterate shop attribute mappings in chunks of 100 sorted by external_id in descending order:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for shop_attribute_mappings in client.shop_attribute_mapping.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for shop_attribute_mapping in shop_attribute_mappings:
                ...         print(shop_attribute_mapping.external_id)

            Iterate shop attribute mappings in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for first_iteration in client.shop_attribute_mapping.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for shop_attribute_mappings in client.shop_attribute_mapping.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for shop_attribute_mapping in shop_attribute_mappings:
                ...         print(shop_attribute_mapping.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_shop_attribute_mapping_filter(
            self._view_id,
            object_type,
            object_type_prefix,
            object_name,
            object_name_prefix,
            attribute_name,
            attribute_name_prefix,
            retrieve,
            retrieve_prefix,
            aggregation,
            aggregation_prefix,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, "skip", cursors=cursors)

    def list(
        self,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ShopAttributeMappingFields | Sequence[ShopAttributeMappingFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ShopAttributeMappingList:
        """List/filter shop attribute mappings

        Args:
            object_type: The object type to filter on.
            object_type_prefix: The prefix of the object type to filter on.
            object_name: The object name to filter on.
            object_name_prefix: The prefix of the object name to filter on.
            attribute_name: The attribute name to filter on.
            attribute_name_prefix: The prefix of the attribute name to filter on.
            retrieve: The retrieve to filter on.
            retrieve_prefix: The prefix of the retrieve to filter on.
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop attribute mappings to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested shop attribute mappings

        Examples:

            List shop attribute mappings and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_attribute_mappings = client.shop_attribute_mapping.list(limit=5)

        """
        filter_ = _create_shop_attribute_mapping_filter(
            self._view_id,
            object_type,
            object_type_prefix,
            object_name,
            object_name_prefix,
            attribute_name,
            attribute_name_prefix,
            retrieve,
            retrieve_prefix,
            aggregation,
            aggregation_prefix,
            external_id_prefix,
            space,
            filter,
        )
        sort_input =  self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        return self._list(limit=limit,  filter=filter_, sort=sort_input)
