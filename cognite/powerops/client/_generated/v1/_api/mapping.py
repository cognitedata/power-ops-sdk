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
    Mapping,
    MappingWrite,
    MappingFields,
    MappingList,
    MappingWriteList,
    MappingTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._mapping import (
    _MAPPING_PROPERTIES_BY_FIELD,
    _create_mapping_filter,
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
from .mapping_timeseries import MappingTimeseriesAPI
from .mapping_query import MappingQueryAPI


class MappingAPI(NodeAPI[Mapping, MappingWrite, MappingList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[Mapping]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Mapping,
            class_list=MappingList,
            class_write_list=MappingWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.timeseries = MappingTimeseriesAPI(client, view_id)

    def __call__(
        self,
        shop_path: str | list[str] | None = None,
        shop_path_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> MappingQueryAPI[MappingList]:
        """Query starting at mappings.

        Args:
            shop_path: The shop path to filter on.
            shop_path_prefix: The prefix of the shop path to filter on.
            retrieve: The retrieve to filter on.
            retrieve_prefix: The prefix of the retrieve to filter on.
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of mappings to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for mappings.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_mapping_filter(
            self._view_id,
            shop_path,
            shop_path_prefix,
            retrieve,
            retrieve_prefix,
            aggregation,
            aggregation_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(MappingList)
        return MappingQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        mapping: MappingWrite | Sequence[MappingWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) mappings.

        Args:
            mapping: Mapping or sequence of mappings to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new mapping:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import MappingWrite
                >>> client = PowerOpsModelsV1Client()
                >>> mapping = MappingWrite(external_id="my_mapping", ...)
                >>> result = client.mapping.apply(mapping)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.mapping.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(mapping, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more mapping.

        Args:
            external_id: External id of the mapping to delete.
            space: The space where all the mapping are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete mapping by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.mapping.delete("my_mapping")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.mapping.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Mapping | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> MappingList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Mapping | MappingList | None:
        """Retrieve one or more mappings by id(s).

        Args:
            external_id: External id or list of external ids of the mappings.
            space: The space where all the mappings are located.

        Returns:
            The requested mappings.

        Examples:

            Retrieve mapping by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> mapping = client.mapping.retrieve("my_mapping")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: MappingTextFields | Sequence[MappingTextFields] | None = None,
        shop_path: str | list[str] | None = None,
        shop_path_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MappingList:
        """Search mappings

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            shop_path: The shop path to filter on.
            shop_path_prefix: The prefix of the shop path to filter on.
            retrieve: The retrieve to filter on.
            retrieve_prefix: The prefix of the retrieve to filter on.
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of mappings to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results mappings matching the query.

        Examples:

           Search for 'my_mapping' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> mappings = client.mapping.search('my_mapping')

        """
        filter_ = _create_mapping_filter(
            self._view_id,
            shop_path,
            shop_path_prefix,
            retrieve,
            retrieve_prefix,
            aggregation,
            aggregation_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _MAPPING_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: MappingFields | Sequence[MappingFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: MappingTextFields | Sequence[MappingTextFields] | None = None,
        shop_path: str | list[str] | None = None,
        shop_path_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
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
        property: MappingFields | Sequence[MappingFields] | None = None,
        group_by: MappingFields | Sequence[MappingFields] = None,
        query: str | None = None,
        search_properties: MappingTextFields | Sequence[MappingTextFields] | None = None,
        shop_path: str | list[str] | None = None,
        shop_path_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
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
        property: MappingFields | Sequence[MappingFields] | None = None,
        group_by: MappingFields | Sequence[MappingFields] | None = None,
        query: str | None = None,
        search_property: MappingTextFields | Sequence[MappingTextFields] | None = None,
        shop_path: str | list[str] | None = None,
        shop_path_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across mappings

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            shop_path: The shop path to filter on.
            shop_path_prefix: The prefix of the shop path to filter on.
            retrieve: The retrieve to filter on.
            retrieve_prefix: The prefix of the retrieve to filter on.
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of mappings to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count mappings in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.mapping.aggregate("count", space="my_space")

        """

        filter_ = _create_mapping_filter(
            self._view_id,
            shop_path,
            shop_path_prefix,
            retrieve,
            retrieve_prefix,
            aggregation,
            aggregation_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _MAPPING_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: MappingFields,
        interval: float,
        query: str | None = None,
        search_property: MappingTextFields | Sequence[MappingTextFields] | None = None,
        shop_path: str | list[str] | None = None,
        shop_path_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for mappings

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            shop_path: The shop path to filter on.
            shop_path_prefix: The prefix of the shop path to filter on.
            retrieve: The retrieve to filter on.
            retrieve_prefix: The prefix of the retrieve to filter on.
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of mappings to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_mapping_filter(
            self._view_id,
            shop_path,
            shop_path_prefix,
            retrieve,
            retrieve_prefix,
            aggregation,
            aggregation_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _MAPPING_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        shop_path: str | list[str] | None = None,
        shop_path_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MappingList:
        """List/filter mappings

        Args:
            shop_path: The shop path to filter on.
            shop_path_prefix: The prefix of the shop path to filter on.
            retrieve: The retrieve to filter on.
            retrieve_prefix: The prefix of the retrieve to filter on.
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of mappings to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested mappings

        Examples:

            List mappings and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> mappings = client.mapping.list(limit=5)

        """
        filter_ = _create_mapping_filter(
            self._view_id,
            shop_path,
            shop_path_prefix,
            retrieve,
            retrieve_prefix,
            aggregation,
            aggregation_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
