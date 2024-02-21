from __future__ import annotations

from collections.abc import Sequence
from typing import overload
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.afrr_bid.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.afrr_bid.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    BidRow,
    BidRowWrite,
    BidRowFields,
    BidRowList,
    BidRowWriteList,
    BidRowTextFields,
)
from cognite.powerops.client._generated.afrr_bid.data_classes._bid_row import (
    _BIDROW_PROPERTIES_BY_FIELD,
    _create_bid_row_filter,
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
from .bid_row_alerts import BidRowAlertsAPI
from .bid_row_query import BidRowQueryAPI


class BidRowAPI(NodeAPI[BidRow, BidRowWrite, BidRowList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[BidRow]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BidRow,
            class_list=BidRowList,
            class_write_list=BidRowWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.alerts_edge = BidRowAlertsAPI(client)

    def __call__(
        self,
        min_price: float | None = None,
        max_price: float | None = None,
        product: str | list[str] | None = None,
        product_prefix: str | None = None,
        is_divisible: bool | None = None,
        is_block: bool | None = None,
        exclusive_group_id: str | list[str] | None = None,
        exclusive_group_id_prefix: str | None = None,
        linked_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> BidRowQueryAPI[BidRowList]:
        """Query starting at bid rows.

        Args:
            min_price: The minimum value of the price to filter on.
            max_price: The maximum value of the price to filter on.
            product: The product to filter on.
            product_prefix: The prefix of the product to filter on.
            is_divisible: The is divisible to filter on.
            is_block: The is block to filter on.
            exclusive_group_id: The exclusive group id to filter on.
            exclusive_group_id_prefix: The prefix of the exclusive group id to filter on.
            linked_bid: The linked bid to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            asset_id: The asset id to filter on.
            asset_id_prefix: The prefix of the asset id to filter on.
            method: The method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid rows to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for bid rows.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_bid_row_filter(
            self._view_id,
            min_price,
            max_price,
            product,
            product_prefix,
            is_divisible,
            is_block,
            exclusive_group_id,
            exclusive_group_id_prefix,
            linked_bid,
            asset_type,
            asset_type_prefix,
            asset_id,
            asset_id_prefix,
            method,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(BidRowList)
        return BidRowQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        bid_row: BidRowWrite | Sequence[BidRowWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) bid rows.

        Note: This method iterates through all nodes and timeseries linked to bid_row and creates them including the edges
        between the nodes. For example, if any of `alerts` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            bid_row: Bid row or sequence of bid rows to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new bid_row:

                >>> from cognite.powerops.client._generated.afrr_bid import AFRRBidAPI
                >>> from cognite.powerops.client._generated.afrr_bid.data_classes import BidRowWrite
                >>> client = AFRRBidAPI()
                >>> bid_row = BidRowWrite(external_id="my_bid_row", ...)
                >>> result = client.bid_row.apply(bid_row)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.bid_row.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(bid_row, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more bid row.

        Args:
            external_id: External id of the bid row to delete.
            space: The space where all the bid row are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete bid_row by id:

                >>> from cognite.powerops.client._generated.afrr_bid import AFRRBidAPI
                >>> client = AFRRBidAPI()
                >>> client.bid_row.delete("my_bid_row")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.bid_row.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> BidRow | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> BidRowList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> BidRow | BidRowList | None:
        """Retrieve one or more bid rows by id(s).

        Args:
            external_id: External id or list of external ids of the bid rows.
            space: The space where all the bid rows are located.

        Returns:
            The requested bid rows.

        Examples:

            Retrieve bid_row by id:

                >>> from cognite.powerops.client._generated.afrr_bid import AFRRBidAPI
                >>> client = AFRRBidAPI()
                >>> bid_row = client.bid_row.retrieve("my_bid_row")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.alerts_edge,
                    "alerts",
                    dm.DirectRelationReference("power-ops-types", "calculationIssue"),
                    "outwards",
                    dm.ViewId("power-ops-shared", "Alert", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: BidRowTextFields | Sequence[BidRowTextFields] | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        product: str | list[str] | None = None,
        product_prefix: str | None = None,
        is_divisible: bool | None = None,
        is_block: bool | None = None,
        exclusive_group_id: str | list[str] | None = None,
        exclusive_group_id_prefix: str | None = None,
        linked_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidRowList:
        """Search bid rows

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            min_price: The minimum value of the price to filter on.
            max_price: The maximum value of the price to filter on.
            product: The product to filter on.
            product_prefix: The prefix of the product to filter on.
            is_divisible: The is divisible to filter on.
            is_block: The is block to filter on.
            exclusive_group_id: The exclusive group id to filter on.
            exclusive_group_id_prefix: The prefix of the exclusive group id to filter on.
            linked_bid: The linked bid to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            asset_id: The asset id to filter on.
            asset_id_prefix: The prefix of the asset id to filter on.
            method: The method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid rows to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results bid rows matching the query.

        Examples:

           Search for 'my_bid_row' in all text properties:

                >>> from cognite.powerops.client._generated.afrr_bid import AFRRBidAPI
                >>> client = AFRRBidAPI()
                >>> bid_rows = client.bid_row.search('my_bid_row')

        """
        filter_ = _create_bid_row_filter(
            self._view_id,
            min_price,
            max_price,
            product,
            product_prefix,
            is_divisible,
            is_block,
            exclusive_group_id,
            exclusive_group_id_prefix,
            linked_bid,
            asset_type,
            asset_type_prefix,
            asset_id,
            asset_id_prefix,
            method,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _BIDROW_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: BidRowFields | Sequence[BidRowFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: BidRowTextFields | Sequence[BidRowTextFields] | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        product: str | list[str] | None = None,
        product_prefix: str | None = None,
        is_divisible: bool | None = None,
        is_block: bool | None = None,
        exclusive_group_id: str | list[str] | None = None,
        exclusive_group_id_prefix: str | None = None,
        linked_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: BidRowFields | Sequence[BidRowFields] | None = None,
        group_by: BidRowFields | Sequence[BidRowFields] = None,
        query: str | None = None,
        search_properties: BidRowTextFields | Sequence[BidRowTextFields] | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        product: str | list[str] | None = None,
        product_prefix: str | None = None,
        is_divisible: bool | None = None,
        is_block: bool | None = None,
        exclusive_group_id: str | list[str] | None = None,
        exclusive_group_id_prefix: str | None = None,
        linked_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: BidRowFields | Sequence[BidRowFields] | None = None,
        group_by: BidRowFields | Sequence[BidRowFields] | None = None,
        query: str | None = None,
        search_property: BidRowTextFields | Sequence[BidRowTextFields] | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        product: str | list[str] | None = None,
        product_prefix: str | None = None,
        is_divisible: bool | None = None,
        is_block: bool | None = None,
        exclusive_group_id: str | list[str] | None = None,
        exclusive_group_id_prefix: str | None = None,
        linked_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across bid rows

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_price: The minimum value of the price to filter on.
            max_price: The maximum value of the price to filter on.
            product: The product to filter on.
            product_prefix: The prefix of the product to filter on.
            is_divisible: The is divisible to filter on.
            is_block: The is block to filter on.
            exclusive_group_id: The exclusive group id to filter on.
            exclusive_group_id_prefix: The prefix of the exclusive group id to filter on.
            linked_bid: The linked bid to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            asset_id: The asset id to filter on.
            asset_id_prefix: The prefix of the asset id to filter on.
            method: The method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid rows to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count bid rows in space `my_space`:

                >>> from cognite.powerops.client._generated.afrr_bid import AFRRBidAPI
                >>> client = AFRRBidAPI()
                >>> result = client.bid_row.aggregate("count", space="my_space")

        """

        filter_ = _create_bid_row_filter(
            self._view_id,
            min_price,
            max_price,
            product,
            product_prefix,
            is_divisible,
            is_block,
            exclusive_group_id,
            exclusive_group_id_prefix,
            linked_bid,
            asset_type,
            asset_type_prefix,
            asset_id,
            asset_id_prefix,
            method,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _BIDROW_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: BidRowFields,
        interval: float,
        query: str | None = None,
        search_property: BidRowTextFields | Sequence[BidRowTextFields] | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        product: str | list[str] | None = None,
        product_prefix: str | None = None,
        is_divisible: bool | None = None,
        is_block: bool | None = None,
        exclusive_group_id: str | list[str] | None = None,
        exclusive_group_id_prefix: str | None = None,
        linked_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for bid rows

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_price: The minimum value of the price to filter on.
            max_price: The maximum value of the price to filter on.
            product: The product to filter on.
            product_prefix: The prefix of the product to filter on.
            is_divisible: The is divisible to filter on.
            is_block: The is block to filter on.
            exclusive_group_id: The exclusive group id to filter on.
            exclusive_group_id_prefix: The prefix of the exclusive group id to filter on.
            linked_bid: The linked bid to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            asset_id: The asset id to filter on.
            asset_id_prefix: The prefix of the asset id to filter on.
            method: The method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid rows to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_bid_row_filter(
            self._view_id,
            min_price,
            max_price,
            product,
            product_prefix,
            is_divisible,
            is_block,
            exclusive_group_id,
            exclusive_group_id_prefix,
            linked_bid,
            asset_type,
            asset_type_prefix,
            asset_id,
            asset_id_prefix,
            method,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _BIDROW_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        min_price: float | None = None,
        max_price: float | None = None,
        product: str | list[str] | None = None,
        product_prefix: str | None = None,
        is_divisible: bool | None = None,
        is_block: bool | None = None,
        exclusive_group_id: str | list[str] | None = None,
        exclusive_group_id_prefix: str | None = None,
        linked_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> BidRowList:
        """List/filter bid rows

        Args:
            min_price: The minimum value of the price to filter on.
            max_price: The maximum value of the price to filter on.
            product: The product to filter on.
            product_prefix: The prefix of the product to filter on.
            is_divisible: The is divisible to filter on.
            is_block: The is block to filter on.
            exclusive_group_id: The exclusive group id to filter on.
            exclusive_group_id_prefix: The prefix of the exclusive group id to filter on.
            linked_bid: The linked bid to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            asset_id: The asset id to filter on.
            asset_id_prefix: The prefix of the asset id to filter on.
            method: The method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid rows to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `alerts` external ids for the bid rows. Defaults to True.

        Returns:
            List of requested bid rows

        Examples:

            List bid rows and limit to 5:

                >>> from cognite.powerops.client._generated.afrr_bid import AFRRBidAPI
                >>> client = AFRRBidAPI()
                >>> bid_rows = client.bid_row.list(limit=5)

        """
        filter_ = _create_bid_row_filter(
            self._view_id,
            min_price,
            max_price,
            product,
            product_prefix,
            is_divisible,
            is_block,
            exclusive_group_id,
            exclusive_group_id_prefix,
            linked_bid,
            asset_type,
            asset_type_prefix,
            asset_id,
            asset_id_prefix,
            method,
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
                    dm.DirectRelationReference("power-ops-types", "calculationIssue"),
                    "outwards",
                    dm.ViewId("power-ops-shared", "Alert", "1"),
                ),
            ],
        )
