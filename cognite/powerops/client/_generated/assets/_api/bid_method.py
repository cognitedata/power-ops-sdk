from __future__ import annotations

from collections.abc import Sequence
from typing import overload
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.assets.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.assets.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    BidMethod,
    BidMethodWrite,
    BidMethodFields,
    BidMethodList,
    BidMethodWriteList,
    BidMethodTextFields,
)
from cognite.powerops.client._generated.assets.data_classes._bid_method import (
    _BIDMETHOD_PROPERTIES_BY_FIELD,
    _create_bid_method_filter,
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
from .bid_method_query import BidMethodQueryAPI


class BidMethodAPI(NodeAPI[BidMethod, BidMethodWrite, BidMethodList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[BidMethod]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BidMethod,
            class_list=BidMethodList,
            class_write_list=BidMethodWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> BidMethodQueryAPI[BidMethodList]:
        """Query starting at bid methods.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid methods to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for bid methods.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_bid_method_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(BidMethodList)
        return BidMethodQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        bid_method: BidMethodWrite | Sequence[BidMethodWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) bid methods.

        Args:
            bid_method: Bid method or sequence of bid methods to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new bid_method:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> from cognite.powerops.client._generated.assets.data_classes import BidMethodWrite
                >>> client = PowerAssetAPI()
                >>> bid_method = BidMethodWrite(external_id="my_bid_method", ...)
                >>> result = client.bid_method.apply(bid_method)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.bid_method.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(bid_method, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more bid method.

        Args:
            external_id: External id of the bid method to delete.
            space: The space where all the bid method are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete bid_method by id:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> client.bid_method.delete("my_bid_method")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.bid_method.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> BidMethod | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> BidMethodList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> BidMethod | BidMethodList | None:
        """Retrieve one or more bid methods by id(s).

        Args:
            external_id: External id or list of external ids of the bid methods.
            space: The space where all the bid methods are located.

        Returns:
            The requested bid methods.

        Examples:

            Retrieve bid_method by id:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> bid_method = client.bid_method.retrieve("my_bid_method")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: BidMethodTextFields | Sequence[BidMethodTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidMethodList:
        """Search bid methods

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid methods to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results bid methods matching the query.

        Examples:

           Search for 'my_bid_method' in all text properties:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> bid_methods = client.bid_method.search('my_bid_method')

        """
        filter_ = _create_bid_method_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _BIDMETHOD_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: BidMethodFields | Sequence[BidMethodFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: BidMethodTextFields | Sequence[BidMethodTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        property: BidMethodFields | Sequence[BidMethodFields] | None = None,
        group_by: BidMethodFields | Sequence[BidMethodFields] = None,
        query: str | None = None,
        search_properties: BidMethodTextFields | Sequence[BidMethodTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        property: BidMethodFields | Sequence[BidMethodFields] | None = None,
        group_by: BidMethodFields | Sequence[BidMethodFields] | None = None,
        query: str | None = None,
        search_property: BidMethodTextFields | Sequence[BidMethodTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across bid methods

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid methods to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count bid methods in space `my_space`:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> result = client.bid_method.aggregate("count", space="my_space")

        """

        filter_ = _create_bid_method_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _BIDMETHOD_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: BidMethodFields,
        interval: float,
        query: str | None = None,
        search_property: BidMethodTextFields | Sequence[BidMethodTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for bid methods

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid methods to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_bid_method_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _BIDMETHOD_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidMethodList:
        """List/filter bid methods

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid methods to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested bid methods

        Examples:

            List bid methods and limit to 5:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> bid_methods = client.bid_method.list(limit=5)

        """
        filter_ = _create_bid_method_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
