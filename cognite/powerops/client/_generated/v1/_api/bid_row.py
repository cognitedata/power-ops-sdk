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
from cognite.powerops.client._generated.v1.data_classes._bid_row import (
    BidRowQuery,
    _BIDROW_PROPERTIES_BY_FIELD,
    _create_bid_row_filter,
)
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    BidRow,
    BidRowWrite,
    BidRowFields,
    BidRowList,
    BidRowWriteList,
    BidRowTextFields,
    Alert,
    PowerAsset,
)
from cognite.powerops.client._generated.v1._api.bid_row_alerts import BidRowAlertsAPI


class BidRowAPI(NodeAPI[BidRow, BidRowWrite, BidRowList, BidRowWriteList]):
    _view_id = dm.ViewId("power_ops_core", "BidRow", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _BIDROW_PROPERTIES_BY_FIELD
    _class_type = BidRow
    _class_list = BidRowList
    _class_write_list = BidRowWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.alerts_edge = BidRowAlertsAPI(client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> BidRow | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> BidRowList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> BidRow | BidRowList | None:
        """Retrieve one or more bid rows by id(s).

        Args:
            external_id: External id or list of external ids of the bid rows.
            space: The space where all the bid rows are located.
            retrieve_connections: Whether to retrieve `linked_bid`, `power_asset` and `alerts` for the bid rows.
            Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier
            of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            The requested bid rows.

        Examples:

            Retrieve bid_row by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_row = client.bid_row.retrieve(
                ...     "my_bid_row"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_connections=retrieve_connections,
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
        linked_bid: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        power_asset: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
            limit: Maximum number of bid rows to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results bid rows matching the query.

        Examples:

           Search for 'my_bid_row' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_rows = client.bid_row.search(
                ...     'my_bid_row'
                ... )

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
        linked_bid: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        power_asset: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
        linked_bid: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        power_asset: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
        linked_bid: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        power_asset: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
        linked_bid: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        power_asset: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
            limit: Maximum number of bid rows to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

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
        linked_bid: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        power_asset: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
            limit: Maximum number of bid rows to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

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

    def select(self) -> BidRowQuery:
        """Start selecting from bid rows."""
        return BidRowQuery(self._client)

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
        if retrieve_connections == "identifier" or retrieve_connections == "full":
            builder.extend(
                factory.from_edge(
                    Alert._view_id,
                    "outwards",
                    ViewPropertyId(self._view_id, "alerts"),
                    include_end_node=retrieve_connections == "full",
                    has_container_fields=True,
                )
            )
        if retrieve_connections == "full":
            builder.extend(
                factory.from_direct_relation(
                    BidRow._view_id,
                    ViewPropertyId(self._view_id, "linkedBid"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    PowerAsset._view_id,
                    ViewPropertyId(self._view_id, "powerAsset"),
                    has_container_fields=True,
                )
            )
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        min_price: float | None = None,
        max_price: float | None = None,
        product: str | list[str] | None = None,
        product_prefix: str | None = None,
        is_divisible: bool | None = None,
        is_block: bool | None = None,
        exclusive_group_id: str | list[str] | None = None,
        exclusive_group_id_prefix: str | None = None,
        linked_bid: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        power_asset: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[BidRowList]:
        """Iterate over bid rows

        Args:
            chunk_size: The number of bid rows to return in each iteration. Defaults to 100.
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
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `linked_bid`, `power_asset` and `alerts` for the bid rows.
            Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier
            of the connected items, and 'full' will retrieve the full connected items.
            limit: Maximum number of bid rows to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of bid rows

        Examples:

            Iterate bid rows in chunks of 100 up to 2000 items:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for bid_rows in client.bid_row.iterate(chunk_size=100, limit=2000):
                ...     for bid_row in bid_rows:
                ...         print(bid_row.external_id)

            Iterate bid rows in chunks of 100 sorted by external_id in descending order:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for bid_rows in client.bid_row.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for bid_row in bid_rows:
                ...         print(bid_row.external_id)

            Iterate bid rows in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for first_iteration in client.bid_row.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for bid_rows in client.bid_row.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for bid_row in bid_rows:
                ...         print(bid_row.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
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
        yield from self._iterate(chunk_size, filter_, limit, retrieve_connections, cursors=cursors)

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
        linked_bid: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        power_asset: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: BidRowFields | Sequence[BidRowFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
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
            limit: Maximum number of bid rows to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `linked_bid`, `power_asset` and `alerts` for the bid rows.
            Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier
            of the connected items, and 'full' will retrieve the full connected items.

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
        sort_input =  self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        if retrieve_connections == "skip":
            return self._list(limit=limit,  filter=filter_, sort=sort_input)
        return self._query(filter_, limit, retrieve_connections, sort_input, "list")
