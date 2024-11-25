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
from cognite.powerops.client._generated.v1.data_classes._bid_configuration_day_ahead import (
    BidConfigurationDayAheadQuery,
    _BIDCONFIGURATIONDAYAHEAD_PROPERTIES_BY_FIELD,
    _create_bid_configuration_day_ahead_filter,
)
from cognite.powerops.client._generated.v1._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite.powerops.client._generated.v1._api.bid_configuration_day_ahead_partials import BidConfigurationDayAheadPartialsAPI
from cognite.powerops.client._generated.v1._api.bid_configuration_day_ahead_query import BidConfigurationDayAheadQueryAPI


class BidConfigurationDayAheadAPI(NodeAPI[BidConfigurationDayAhead, BidConfigurationDayAheadWrite, BidConfigurationDayAheadList, BidConfigurationDayAheadWriteList]):
    _view_id = dm.ViewId("power_ops_core", "BidConfigurationDayAhead", "1")
    _properties_by_field = _BIDCONFIGURATIONDAYAHEAD_PROPERTIES_BY_FIELD
    _class_type = BidConfigurationDayAhead
    _class_list = BidConfigurationDayAheadList
    _class_write_list = BidConfigurationDayAheadWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.partials_edge = BidConfigurationDayAheadPartialsAPI(client)

    def __call__(
            self,
            name: str | list[str] | None = None,
            name_prefix: str | None = None,
            market_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
            price_area: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
            bid_date_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit: int = DEFAULT_QUERY_LIMIT,
            filter: dm.Filter | None = None,
    ) -> BidConfigurationDayAheadQueryAPI[BidConfigurationDayAheadList]:
        """Query starting at bid configuration day aheads.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            market_configuration: The market configuration to filter on.
            price_area: The price area to filter on.
            bid_date_specification: The bid date specification to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid configuration day aheads to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for bid configuration day aheads.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. "
            "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_bid_configuration_day_ahead_filter(
            self._view_id,
            name,
            name_prefix,
            market_configuration,
            price_area,
            bid_date_specification,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(BidConfigurationDayAheadList)
        return BidConfigurationDayAheadQueryAPI(self._client, builder, filter_, limit)


    def apply(
        self,
        bid_configuration_day_ahead: BidConfigurationDayAheadWrite | Sequence[BidConfigurationDayAheadWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) bid configuration day aheads.

        Note: This method iterates through all nodes and timeseries linked to bid_configuration_day_ahead and creates them including the edges
        between the nodes. For example, if any of `market_configuration`, `price_area`, `bid_date_specification` or `partials` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            bid_configuration_day_ahead: Bid configuration day ahead or sequence of bid configuration day aheads to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new bid_configuration_day_ahead:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import BidConfigurationDayAheadWrite
                >>> client = PowerOpsModelsV1Client()
                >>> bid_configuration_day_ahead = BidConfigurationDayAheadWrite(external_id="my_bid_configuration_day_ahead", ...)
                >>> result = client.bid_configuration_day_ahead.apply(bid_configuration_day_ahead)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.bid_configuration_day_ahead.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(bid_configuration_day_ahead, replace, write_none)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> dm.InstancesDeleteResult:
        """Delete one or more bid configuration day ahead.

        Args:
            external_id: External id of the bid configuration day ahead to delete.
            space: The space where all the bid configuration day ahead are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete bid_configuration_day_ahead by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.bid_configuration_day_ahead.delete("my_bid_configuration_day_ahead")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.bid_configuration_day_ahead.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE) -> BidConfigurationDayAhead | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE) -> BidConfigurationDayAheadList:
        ...

    def retrieve(self, external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE) -> BidConfigurationDayAhead | BidConfigurationDayAheadList | None:
        """Retrieve one or more bid configuration day aheads by id(s).

        Args:
            external_id: External id or list of external ids of the bid configuration day aheads.
            space: The space where all the bid configuration day aheads are located.

        Returns:
            The requested bid configuration day aheads.

        Examples:

            Retrieve bid_configuration_day_ahead by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_configuration_day_ahead = client.bid_configuration_day_ahead.retrieve("my_bid_configuration_day_ahead")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.partials_edge,
                    "partials",
                    dm.DirectRelationReference("power_ops_types", "BidConfiguration.partials"),
                    "outwards",
                    dm.ViewId("power_ops_core", "PartialBidConfiguration", "1"),
                ),
                                               ]
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
            limit: Maximum number of bid configuration day aheads to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results bid configuration day aheads matching the query.

        Examples:

           Search for 'my_bid_configuration_day_ahead' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_configuration_day_aheads = client.bid_configuration_day_ahead.search('my_bid_configuration_day_ahead')

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
    ) -> dm.aggregations.AggregatedNumberedValue:
        ...

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
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

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
    ) -> InstanceAggregationResultList:
        ...

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
            limit: Maximum number of bid configuration day aheads to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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
            limit: Maximum number of bid configuration day aheads to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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

    def query(self) -> BidConfigurationDayAheadQuery:
        """Start a query for bid configuration day aheads."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return BidConfigurationDayAheadQuery(self._client)

    def select(self) -> BidConfigurationDayAheadQuery:
        """Start selecting from bid configuration day aheads."""
        warnings.warn("The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2)
        return BidConfigurationDayAheadQuery(self._client)

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
            limit: Maximum number of bid configuration day aheads to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `market_configuration`, `price_area`, `bid_date_specification` and `partials` for the bid configuration day aheads. Defaults to 'skip'.
                'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

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

        if retrieve_connections == "skip":
                return self._list(
                limit=limit,
                filter=filter_,
                sort_by=sort_by,  # type: ignore[arg-type]
                direction=direction,
                sort=sort,
            )

        builder = DataClassQueryBuilder(BidConfigurationDayAheadList)
        has_data = dm.filters.HasData(views=[self._view_id])
        builder.append(
            NodeQueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=dm.filters.And(filter_, has_data) if filter_ else has_data,
                    sort=self._create_sort(sort_by, direction, sort),  # type: ignore[arg-type]
                ),
                BidConfigurationDayAhead,
                max_retrieve_limit=limit,
                raw_filter=filter_,
            )
        )
        from_root = builder.get_from()
        edge_partials = builder.create_name(from_root)
        builder.append(
            EdgeQueryStep(
                edge_partials,
                dm.query.EdgeResultSetExpression(
                    from_=from_root,
                    direction="outwards",
                    chain_to="destination",
                ),
            )
        )
        if retrieve_connections == "full":
            builder.append(
                NodeQueryStep(
                    builder.create_name( edge_partials),
                    dm.query.NodeResultSetExpression(
                        from_= edge_partials,
                        filter=dm.filters.HasData(views=[PartialBidConfiguration._view_id]),
                    ),
                    PartialBidConfiguration,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[MarketConfiguration._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("marketConfiguration"),
                    ),
                    MarketConfiguration,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[PriceAreaDayAhead._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("priceArea"),
                    ),
                    PriceAreaDayAhead,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[DateSpecification._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("bidDateSpecification"),
                    ),
                    DateSpecification,
                )
            )
        # We know that that all nodes are connected as it is not possible to filter on connections
        builder.execute_query(self._client, remove_not_connected=False)
        return builder.unpack()
