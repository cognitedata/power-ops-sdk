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
from cognite.powerops.client._generated.v1.data_classes._bid_configuration_day_ahead import (
    BidConfigurationDayAheadQuery,
    _BIDCONFIGURATIONDAYAHEAD_PROPERTIES_BY_FIELD,
    _create_bid_configuration_day_ahead_filter,
)
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    BidConfigurationDayAhead,
    BidConfigurationDayAheadWrite,
    BidConfigurationDayAheadFields,
    BidConfigurationDayAheadList,
    BidConfigurationDayAheadWriteList,
    BidConfigurationDayAheadTextFields,
    DateSpecification,
    MarketConfiguration,
    PartialBidConfiguration,
    PriceAreaDayAhead,
)
from cognite.powerops.client._generated.v1._api.bid_configuration_day_ahead_partials import BidConfigurationDayAheadPartialsAPI


class BidConfigurationDayAheadAPI(NodeAPI[BidConfigurationDayAhead, BidConfigurationDayAheadWrite, BidConfigurationDayAheadList, BidConfigurationDayAheadWriteList]):
    _view_id = dm.ViewId("power_ops_core", "BidConfigurationDayAhead", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _BIDCONFIGURATIONDAYAHEAD_PROPERTIES_BY_FIELD
    _class_type = BidConfigurationDayAhead
    _class_list = BidConfigurationDayAheadList
    _class_write_list = BidConfigurationDayAheadWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.partials_edge = BidConfigurationDayAheadPartialsAPI(client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> BidConfigurationDayAhead | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> BidConfigurationDayAheadList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> BidConfigurationDayAhead | BidConfigurationDayAheadList | None:
        """Retrieve one or more bid configuration day aheads by id(s).

        Args:
            external_id: External id or list of external ids of the bid configuration day aheads.
            space: The space where all the bid configuration day aheads are located.
            retrieve_connections: Whether to retrieve `market_configuration`, `price_area`, `bid_date_specification` and
            `partials` for the bid configuration day aheads. Defaults to 'skip'.'skip' will not retrieve any
            connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve
            the full connected items.

        Returns:
            The requested bid configuration day aheads.

        Examples:

            Retrieve bid_configuration_day_ahead by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_configuration_day_ahead = client.bid_configuration_day_ahead.retrieve(
                ...     "my_bid_configuration_day_ahead"
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
        properties: BidConfigurationDayAheadTextFields | SequenceNotStr[BidConfigurationDayAheadTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        price_area: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        bid_date_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: BidConfigurationDayAheadFields | SequenceNotStr[BidConfigurationDayAheadFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> BidConfigurationDayAheadList:
        """Search bid configuration day aheads

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            market_configuration: The market configuration to filter on.
            price_area: The price area to filter on.
            bid_date_specification: The bid date specification to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid configuration day aheads to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results bid configuration day aheads matching the query.

        Examples:

           Search for 'my_bid_configuration_day_ahead' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_configuration_day_aheads = client.bid_configuration_day_ahead.search(
                ...     'my_bid_configuration_day_ahead'
                ... )

        """
        filter_ = _create_bid_configuration_day_ahead_filter(
            self._view_id,
            name,
            name_prefix,
            market_configuration,
            price_area,
            bid_date_specification,
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
        property: BidConfigurationDayAheadFields | SequenceNotStr[BidConfigurationDayAheadFields] | None = None,
        query: str | None = None,
        search_property: BidConfigurationDayAheadTextFields | SequenceNotStr[BidConfigurationDayAheadTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        price_area: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        bid_date_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
        property: BidConfigurationDayAheadFields | SequenceNotStr[BidConfigurationDayAheadFields] | None = None,
        query: str | None = None,
        search_property: BidConfigurationDayAheadTextFields | SequenceNotStr[BidConfigurationDayAheadTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        price_area: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        bid_date_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
        group_by: BidConfigurationDayAheadFields | SequenceNotStr[BidConfigurationDayAheadFields],
        property: BidConfigurationDayAheadFields | SequenceNotStr[BidConfigurationDayAheadFields] | None = None,
        query: str | None = None,
        search_property: BidConfigurationDayAheadTextFields | SequenceNotStr[BidConfigurationDayAheadTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        price_area: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        bid_date_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
        group_by: BidConfigurationDayAheadFields | SequenceNotStr[BidConfigurationDayAheadFields] | None = None,
        property: BidConfigurationDayAheadFields | SequenceNotStr[BidConfigurationDayAheadFields] | None = None,
        query: str | None = None,
        search_property: BidConfigurationDayAheadTextFields | SequenceNotStr[BidConfigurationDayAheadTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        price_area: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        bid_date_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across bid configuration day aheads

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            market_configuration: The market configuration to filter on.
            price_area: The price area to filter on.
            bid_date_specification: The bid date specification to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid configuration day aheads to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count bid configuration day aheads in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.bid_configuration_day_ahead.aggregate("count", space="my_space")

        """

        filter_ = _create_bid_configuration_day_ahead_filter(
            self._view_id,
            name,
            name_prefix,
            market_configuration,
            price_area,
            bid_date_specification,
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
        property: BidConfigurationDayAheadFields,
        interval: float,
        query: str | None = None,
        search_property: BidConfigurationDayAheadTextFields | SequenceNotStr[BidConfigurationDayAheadTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        price_area: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        bid_date_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for bid configuration day aheads

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            market_configuration: The market configuration to filter on.
            price_area: The price area to filter on.
            bid_date_specification: The bid date specification to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid configuration day aheads to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_bid_configuration_day_ahead_filter(
            self._view_id,
            name,
            name_prefix,
            market_configuration,
            price_area,
            bid_date_specification,
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

    def select(self) -> BidConfigurationDayAheadQuery:
        """Start selecting from bid configuration day aheads."""
        return BidConfigurationDayAheadQuery(self._client)

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
                    PartialBidConfiguration._view_id,
                    "outwards",
                    ViewPropertyId(self._view_id, "partials"),
                    include_end_node=retrieve_connections == "full",
                    has_container_fields=True,
                )
            )
        if retrieve_connections == "full":
            builder.extend(
                factory.from_direct_relation(
                    MarketConfiguration._view_id,
                    ViewPropertyId(self._view_id, "marketConfiguration"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    PriceAreaDayAhead._view_id,
                    ViewPropertyId(self._view_id, "priceArea"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    DateSpecification._view_id,
                    ViewPropertyId(self._view_id, "bidDateSpecification"),
                    has_container_fields=True,
                )
            )
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        price_area: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        bid_date_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[BidConfigurationDayAheadList]:
        """Iterate over bid configuration day aheads

        Args:
            chunk_size: The number of bid configuration day aheads to return in each iteration. Defaults to 100.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            market_configuration: The market configuration to filter on.
            price_area: The price area to filter on.
            bid_date_specification: The bid date specification to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `market_configuration`, `price_area`, `bid_date_specification` and
            `partials` for the bid configuration day aheads. Defaults to 'skip'.'skip' will not retrieve any
            connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve
            the full connected items.
            limit: Maximum number of bid configuration day aheads to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of bid configuration day aheads

        Examples:

            Iterate bid configuration day aheads in chunks of 100 up to 2000 items:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for bid_configuration_day_aheads in client.bid_configuration_day_ahead.iterate(chunk_size=100, limit=2000):
                ...     for bid_configuration_day_ahead in bid_configuration_day_aheads:
                ...         print(bid_configuration_day_ahead.external_id)

            Iterate bid configuration day aheads in chunks of 100 sorted by external_id in descending order:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for bid_configuration_day_aheads in client.bid_configuration_day_ahead.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for bid_configuration_day_ahead in bid_configuration_day_aheads:
                ...         print(bid_configuration_day_ahead.external_id)

            Iterate bid configuration day aheads in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for first_iteration in client.bid_configuration_day_ahead.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for bid_configuration_day_aheads in client.bid_configuration_day_ahead.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for bid_configuration_day_ahead in bid_configuration_day_aheads:
                ...         print(bid_configuration_day_ahead.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_bid_configuration_day_ahead_filter(
            self._view_id,
            name,
            name_prefix,
            market_configuration,
            price_area,
            bid_date_specification,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, retrieve_connections, cursors=cursors)

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        price_area: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        bid_date_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: BidConfigurationDayAheadFields | Sequence[BidConfigurationDayAheadFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> BidConfigurationDayAheadList:
        """List/filter bid configuration day aheads

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            market_configuration: The market configuration to filter on.
            price_area: The price area to filter on.
            bid_date_specification: The bid date specification to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid configuration day aheads to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `market_configuration`, `price_area`, `bid_date_specification` and
            `partials` for the bid configuration day aheads. Defaults to 'skip'.'skip' will not retrieve any
            connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve
            the full connected items.

        Returns:
            List of requested bid configuration day aheads

        Examples:

            List bid configuration day aheads and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_configuration_day_aheads = client.bid_configuration_day_ahead.list(limit=5)

        """
        filter_ = _create_bid_configuration_day_ahead_filter(
            self._view_id,
            name,
            name_prefix,
            market_configuration,
            price_area,
            bid_date_specification,
            external_id_prefix,
            space,
            filter,
        )
        sort_input =  self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        if retrieve_connections == "skip":
            return self._list(limit=limit,  filter=filter_, sort=sort_input)
        return self._query(filter_, limit, retrieve_connections, sort_input, "list")
