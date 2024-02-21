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
    WatercourseShop,
    WatercourseShopWrite,
    WatercourseShopFields,
    WatercourseShopList,
    WatercourseShopWriteList,
    WatercourseShopTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._watercourse_shop import (
    _WATERCOURSESHOP_PROPERTIES_BY_FIELD,
    _create_watercourse_shop_filter,
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
from .watercourse_shop_query import WatercourseShopQueryAPI


class WatercourseShopAPI(NodeAPI[WatercourseShop, WatercourseShopWrite, WatercourseShopList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[WatercourseShop]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WatercourseShop,
            class_list=WatercourseShopList,
            class_write_list=WatercourseShopWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> WatercourseShopQueryAPI[WatercourseShopList]:
        """Query starting at watercourse shops.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of watercourse shops to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for watercourse shops.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_watercourse_shop_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(WatercourseShopList)
        return WatercourseShopQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        watercourse_shop: WatercourseShopWrite | Sequence[WatercourseShopWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) watercourse shops.

        Args:
            watercourse_shop: Watercourse shop or sequence of watercourse shops to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new watercourse_shop:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import WatercourseShopWrite
                >>> client = PowerOpsModelsV1Client()
                >>> watercourse_shop = WatercourseShopWrite(external_id="my_watercourse_shop", ...)
                >>> result = client.watercourse_shop.apply(watercourse_shop)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.watercourse_shop.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(watercourse_shop, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more watercourse shop.

        Args:
            external_id: External id of the watercourse shop to delete.
            space: The space where all the watercourse shop are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete watercourse_shop by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.watercourse_shop.delete("my_watercourse_shop")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.watercourse_shop.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> WatercourseShop | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> WatercourseShopList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> WatercourseShop | WatercourseShopList | None:
        """Retrieve one or more watercourse shops by id(s).

        Args:
            external_id: External id or list of external ids of the watercourse shops.
            space: The space where all the watercourse shops are located.

        Returns:
            The requested watercourse shops.

        Examples:

            Retrieve watercourse_shop by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> watercourse_shop = client.watercourse_shop.retrieve("my_watercourse_shop")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: WatercourseShopTextFields | Sequence[WatercourseShopTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WatercourseShopList:
        """Search watercourse shops

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of watercourse shops to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results watercourse shops matching the query.

        Examples:

           Search for 'my_watercourse_shop' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> watercourse_shops = client.watercourse_shop.search('my_watercourse_shop')

        """
        filter_ = _create_watercourse_shop_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _WATERCOURSESHOP_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: WatercourseShopFields | Sequence[WatercourseShopFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: WatercourseShopTextFields | Sequence[WatercourseShopTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
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
        property: WatercourseShopFields | Sequence[WatercourseShopFields] | None = None,
        group_by: WatercourseShopFields | Sequence[WatercourseShopFields] = None,
        query: str | None = None,
        search_properties: WatercourseShopTextFields | Sequence[WatercourseShopTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
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
        property: WatercourseShopFields | Sequence[WatercourseShopFields] | None = None,
        group_by: WatercourseShopFields | Sequence[WatercourseShopFields] | None = None,
        query: str | None = None,
        search_property: WatercourseShopTextFields | Sequence[WatercourseShopTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across watercourse shops

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of watercourse shops to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count watercourse shops in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.watercourse_shop.aggregate("count", space="my_space")

        """

        filter_ = _create_watercourse_shop_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _WATERCOURSESHOP_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: WatercourseShopFields,
        interval: float,
        query: str | None = None,
        search_property: WatercourseShopTextFields | Sequence[WatercourseShopTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for watercourse shops

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of watercourse shops to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_watercourse_shop_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _WATERCOURSESHOP_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WatercourseShopList:
        """List/filter watercourse shops

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of watercourse shops to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested watercourse shops

        Examples:

            List watercourse shops and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> watercourse_shops = client.watercourse_shop.list(limit=5)

        """
        filter_ = _create_watercourse_shop_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
