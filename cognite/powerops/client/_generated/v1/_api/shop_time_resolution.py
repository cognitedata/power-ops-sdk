from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    NodeQueryStep,
    EdgeQueryStep,
    DataClassQueryBuilder,
)
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    ShopTimeResolution,
    ShopTimeResolutionWrite,
    ShopTimeResolutionFields,
    ShopTimeResolutionList,
    ShopTimeResolutionWriteList,
    ShopTimeResolutionTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._shop_time_resolution import (
    ShopTimeResolutionQuery,
    _SHOPTIMERESOLUTION_PROPERTIES_BY_FIELD,
    _create_shop_time_resolution_filter,
)
from cognite.powerops.client._generated.v1._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite.powerops.client._generated.v1._api.shop_time_resolution_query import ShopTimeResolutionQueryAPI


class ShopTimeResolutionAPI(NodeAPI[ShopTimeResolution, ShopTimeResolutionWrite, ShopTimeResolutionList, ShopTimeResolutionWriteList]):
    _view_id = dm.ViewId("power_ops_core", "ShopTimeResolution", "1")
    _properties_by_field = _SHOPTIMERESOLUTION_PROPERTIES_BY_FIELD
    _class_type = ShopTimeResolution
    _class_list = ShopTimeResolutionList
    _class_write_list = ShopTimeResolutionWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)


    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ShopTimeResolutionQueryAPI[ShopTimeResolutionList]:
        """Query starting at shop time resolutions.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop time resolutions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for shop time resolutions.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. "
            "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_shop_time_resolution_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(ShopTimeResolutionList)
        return ShopTimeResolutionQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        shop_time_resolution: ShopTimeResolutionWrite | Sequence[ShopTimeResolutionWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) shop time resolutions.

        Args:
            shop_time_resolution: Shop time resolution or sequence of shop time resolutions to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new shop_time_resolution:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import ShopTimeResolutionWrite
                >>> client = PowerOpsModelsV1Client()
                >>> shop_time_resolution = ShopTimeResolutionWrite(external_id="my_shop_time_resolution", ...)
                >>> result = client.shop_time_resolution.apply(shop_time_resolution)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.shop_time_resolution.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(shop_time_resolution, replace, write_none)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> dm.InstancesDeleteResult:
        """Delete one or more shop time resolution.

        Args:
            external_id: External id of the shop time resolution to delete.
            space: The space where all the shop time resolution are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete shop_time_resolution by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.shop_time_resolution.delete("my_shop_time_resolution")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.shop_time_resolution.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE) -> ShopTimeResolution | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE) -> ShopTimeResolutionList:
        ...

    def retrieve(self, external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE) -> ShopTimeResolution | ShopTimeResolutionList | None:
        """Retrieve one or more shop time resolutions by id(s).

        Args:
            external_id: External id or list of external ids of the shop time resolutions.
            space: The space where all the shop time resolutions are located.

        Returns:
            The requested shop time resolutions.

        Examples:

            Retrieve shop_time_resolution by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_time_resolution = client.shop_time_resolution.retrieve("my_shop_time_resolution")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: ShopTimeResolutionTextFields | SequenceNotStr[ShopTimeResolutionTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ShopTimeResolutionFields | SequenceNotStr[ShopTimeResolutionFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ShopTimeResolutionList:
        """Search shop time resolutions

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop time resolutions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results shop time resolutions matching the query.

        Examples:

           Search for 'my_shop_time_resolution' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_time_resolutions = client.shop_time_resolution.search('my_shop_time_resolution')

        """
        filter_ = _create_shop_time_resolution_filter(
            self._view_id,
            name,
            name_prefix,
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
        property: ShopTimeResolutionFields | SequenceNotStr[ShopTimeResolutionFields] | None = None,
        query: str | None = None,
        search_property: ShopTimeResolutionTextFields | SequenceNotStr[ShopTimeResolutionTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        property: ShopTimeResolutionFields | SequenceNotStr[ShopTimeResolutionFields] | None = None,
        query: str | None = None,
        search_property: ShopTimeResolutionTextFields | SequenceNotStr[ShopTimeResolutionTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        group_by: ShopTimeResolutionFields | SequenceNotStr[ShopTimeResolutionFields],
        property: ShopTimeResolutionFields | SequenceNotStr[ShopTimeResolutionFields] | None = None,
        query: str | None = None,
        search_property: ShopTimeResolutionTextFields | SequenceNotStr[ShopTimeResolutionTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        group_by: ShopTimeResolutionFields | SequenceNotStr[ShopTimeResolutionFields] | None = None,
        property: ShopTimeResolutionFields | SequenceNotStr[ShopTimeResolutionFields] | None = None,
        query: str | None = None,
        search_property: ShopTimeResolutionTextFields | SequenceNotStr[ShopTimeResolutionTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across shop time resolutions

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop time resolutions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count shop time resolutions in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.shop_time_resolution.aggregate("count", space="my_space")

        """

        filter_ = _create_shop_time_resolution_filter(
            self._view_id,
            name,
            name_prefix,
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
        property: ShopTimeResolutionFields,
        interval: float,
        query: str | None = None,
        search_property: ShopTimeResolutionTextFields | SequenceNotStr[ShopTimeResolutionTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for shop time resolutions

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop time resolutions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_shop_time_resolution_filter(
            self._view_id,
            name,
            name_prefix,
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

    def query(self) -> ShopTimeResolutionQuery:
        """Start a query for shop time resolutions."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return ShopTimeResolutionQuery(self._client)

    def select(self) -> ShopTimeResolutionQuery:
        """Start selecting from shop time resolutions."""
        warnings.warn("The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2)
        return ShopTimeResolutionQuery(self._client)

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ShopTimeResolutionFields | Sequence[ShopTimeResolutionFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ShopTimeResolutionList:
        """List/filter shop time resolutions

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop time resolutions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested shop time resolutions

        Examples:

            List shop time resolutions and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_time_resolutions = client.shop_time_resolution.list(limit=5)

        """
        filter_ = _create_shop_time_resolution_filter(
            self._view_id,
            name,
            name_prefix,
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
