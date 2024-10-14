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
    BidRow,
    BidRowWrite,
    BidRowFields,
    BidRowList,
    BidRowWriteList,
    BidRowTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._bid_row import (
    _BIDROW_PROPERTIES_BY_FIELD,
    _create_bid_row_filter,
)
from ._core import DEFAULT_LIMIT_READ, DEFAULT_QUERY_LIMIT, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .bid_row_alerts import BidRowAlertsAPI
from .bid_row_query import BidRowQueryAPI


class BidRowAPI(NodeAPI[BidRow, BidRowWrite, BidRowList, BidRowWriteList]):
    _view_id = dm.ViewId("power_ops_core", "BidRow", "1")
    _properties_by_field = _BIDROW_PROPERTIES_BY_FIELD
    _class_type = BidRow
    _class_list = BidRowList
    _class_write_list = BidRowWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

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
            power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit: int = DEFAULT_QUERY_LIMIT,
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
            power_asset: The power asset to filter on.
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
            power_asset,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(BidRowList)
        return BidRowQueryAPI(self._client, builder, filter_, limit)


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

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import BidRowWrite
                >>> client = PowerOpsModelsV1Client()
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

    def delete(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> dm.InstancesDeleteResult:
        """Delete one or more bid row.

        Args:
            external_id: External id of the bid row to delete.
            space: The space where all the bid row are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete bid_row by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
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
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> BidRow | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> BidRowList:
        ...

    def retrieve(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> BidRow | BidRowList | None:
        """Retrieve one or more bid rows by id(s).

        Args:
            external_id: External id or list of external ids of the bid rows.
            space: The space where all the bid rows are located.

        Returns:
            The requested bid rows.

        Examples:

            Retrieve bid_row by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
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
                    dm.DirectRelationReference("power_ops_types", "calculationIssue"),
                    "outwards",
                    dm.ViewId("power_ops_core", "Alert", "1"),
                ),
                                               ]
        )


    def search(
        self,
        query: str,
        properties: BidRowTextFields | SequenceNotStr[BidRowTextFields] | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        product: str | list[str] | None = None,
        product_prefix: str | None = None,
        is_divisible: bool | None = None,
        is_block: bool | None = None,
        exclusive_group_id: str | list[str] | None = None,
        exclusive_group_id_prefix: str | None = None,
        linked_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: BidRowFields | SequenceNotStr[BidRowFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
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
            power_asset: The power asset to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid rows to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results bid rows matching the query.

        Examples:

           Search for 'my_bid_row' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
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
            power_asset,
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
        property: BidRowFields | SequenceNotStr[BidRowFields] | None = None,
        query: str | None = None,
        search_property: BidRowTextFields | SequenceNotStr[BidRowTextFields] | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        product: str | list[str] | None = None,
        product_prefix: str | None = None,
        is_divisible: bool | None = None,
        is_block: bool | None = None,
        exclusive_group_id: str | list[str] | None = None,
        exclusive_group_id_prefix: str | None = None,
        linked_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: BidRowFields | SequenceNotStr[BidRowFields] | None = None,
        query: str | None = None,
        search_property: BidRowTextFields | SequenceNotStr[BidRowTextFields] | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        product: str | list[str] | None = None,
        product_prefix: str | None = None,
        is_divisible: bool | None = None,
        is_block: bool | None = None,
        exclusive_group_id: str | list[str] | None = None,
        exclusive_group_id_prefix: str | None = None,
        linked_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        group_by: BidRowFields | SequenceNotStr[BidRowFields],
        property: BidRowFields | SequenceNotStr[BidRowFields] | None = None,
        query: str | None = None,
        search_property: BidRowTextFields | SequenceNotStr[BidRowTextFields] | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        product: str | list[str] | None = None,
        product_prefix: str | None = None,
        is_divisible: bool | None = None,
        is_block: bool | None = None,
        exclusive_group_id: str | list[str] | None = None,
        exclusive_group_id_prefix: str | None = None,
        linked_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        group_by: BidRowFields | SequenceNotStr[BidRowFields] | None = None,
        property: BidRowFields | SequenceNotStr[BidRowFields] | None = None,
        query: str | None = None,
        search_property: BidRowTextFields | SequenceNotStr[BidRowTextFields] | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        product: str | list[str] | None = None,
        product_prefix: str | None = None,
        is_divisible: bool | None = None,
        is_block: bool | None = None,
        exclusive_group_id: str | list[str] | None = None,
        exclusive_group_id_prefix: str | None = None,
        linked_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across bid rows

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
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
            power_asset: The power asset to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid rows to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count bid rows in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
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
            power_asset,
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
        property: BidRowFields,
        interval: float,
        query: str | None = None,
        search_property: BidRowTextFields | SequenceNotStr[BidRowTextFields] | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        product: str | list[str] | None = None,
        product_prefix: str | None = None,
        is_divisible: bool | None = None,
        is_block: bool | None = None,
        exclusive_group_id: str | list[str] | None = None,
        exclusive_group_id_prefix: str | None = None,
        linked_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
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
            power_asset: The power asset to filter on.
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
            power_asset,
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
        min_price: float | None = None,
        max_price: float | None = None,
        product: str | list[str] | None = None,
        product_prefix: str | None = None,
        is_divisible: bool | None = None,
        is_block: bool | None = None,
        exclusive_group_id: str | list[str] | None = None,
        exclusive_group_id_prefix: str | None = None,
        linked_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: BidRowFields | Sequence[BidRowFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
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
            power_asset: The power asset to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid rows to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_edges: Whether to retrieve `alerts` external ids for the bid rows. Defaults to True.

        Returns:
            List of requested bid rows

        Examples:

            List bid rows and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
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
            power_asset,
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
            retrieve_edges=retrieve_edges,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.alerts_edge,
                    "alerts",
                    dm.DirectRelationReference("power_ops_types", "calculationIssue"),
                    "outwards",
                    dm.ViewId("power_ops_core", "Alert", "1"),
                ),
                                               ]
        )
