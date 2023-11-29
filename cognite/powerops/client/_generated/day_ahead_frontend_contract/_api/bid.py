from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    Bid,
    BidApply,
    BidFields,
    BidList,
    BidApplyList,
    BidTextFields,
)
from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes._bid import (
    _BID_PROPERTIES_BY_FIELD,
    _create_bid_filter,
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
from .bid_alerts import BidAlertsAPI
from .bid_partials import BidPartialsAPI
from .bid_query import BidQueryAPI


class BidAPI(NodeAPI[Bid, BidApply, BidList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[BidApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Bid,
            class_apply_type=BidApply,
            class_list=BidList,
            class_apply_list=BidApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.alerts_edge = BidAlertsAPI(client)
        self.partials_edge = BidPartialsAPI(client)

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_start_calculation: datetime.datetime | None = None,
        max_start_calculation: datetime.datetime | None = None,
        min_end_calculation: datetime.datetime | None = None,
        max_end_calculation: datetime.datetime | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> BidQueryAPI[BidList]:
        """Query starting at bids.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            method: The method to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            total: The total to filter on.
            min_start_calculation: The minimum value of the start calculation to filter on.
            max_start_calculation: The maximum value of the start calculation to filter on.
            min_end_calculation: The minimum value of the end calculation to filter on.
            max_end_calculation: The maximum value of the end calculation to filter on.
            market: The market to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bids to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for bids.

        """
        filter_ = _create_bid_filter(
            self._view_id,
            name,
            name_prefix,
            method,
            price_area,
            price_area_prefix,
            min_date,
            max_date,
            total,
            min_start_calculation,
            max_start_calculation,
            min_end_calculation,
            max_end_calculation,
            market,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            BidList,
            [
                QueryStep(
                    name="bid",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_BID_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=Bid,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return BidQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(self, bid: BidApply | Sequence[BidApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) bids.

        Note: This method iterates through all nodes and timeseries linked to bid and creates them including the edges
        between the nodes. For example, if any of `alerts` or `partials` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            bid: Bid or sequence of bids to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new bid:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes import BidApply
                >>> client = DayAheadFrontendContractAPI()
                >>> bid = BidApply(external_id="my_bid", ...)
                >>> result = client.bid.apply(bid)

        """
        return self._apply(bid, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "poweropsDayAheadFrontendContractModel"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more bid.

        Args:
            external_id: External id of the bid to delete.
            space: The space where all the bid are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete bid by id:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> client.bid.delete("my_bid")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> Bid | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> BidList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "poweropsDayAheadFrontendContractModel"
    ) -> Bid | BidList | None:
        """Retrieve one or more bids by id(s).

        Args:
            external_id: External id or list of external ids of the bids.
            space: The space where all the bids are located.

        Returns:
            The requested bids.

        Examples:

            Retrieve bid by id:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> bid = client.bid.retrieve("my_bid")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_triple=[
                (
                    self.alerts_edge,
                    "alerts",
                    dm.DirectRelationReference("poweropsDayAheadFrontendContractModel", "Bid.alerts"),
                ),
                (
                    self.partials_edge,
                    "partials",
                    dm.DirectRelationReference("poweropsDayAheadFrontendContractModel", "Bid.partials"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: BidTextFields | Sequence[BidTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_start_calculation: datetime.datetime | None = None,
        max_start_calculation: datetime.datetime | None = None,
        min_end_calculation: datetime.datetime | None = None,
        max_end_calculation: datetime.datetime | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidList:
        """Search bids

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            method: The method to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            total: The total to filter on.
            min_start_calculation: The minimum value of the start calculation to filter on.
            max_start_calculation: The maximum value of the start calculation to filter on.
            min_end_calculation: The minimum value of the end calculation to filter on.
            max_end_calculation: The maximum value of the end calculation to filter on.
            market: The market to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bids to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results bids matching the query.

        Examples:

           Search for 'my_bid' in all text properties:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> bids = client.bid.search('my_bid')

        """
        filter_ = _create_bid_filter(
            self._view_id,
            name,
            name_prefix,
            method,
            price_area,
            price_area_prefix,
            min_date,
            max_date,
            total,
            min_start_calculation,
            max_start_calculation,
            min_end_calculation,
            max_end_calculation,
            market,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _BID_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: BidFields | Sequence[BidFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: BidTextFields | Sequence[BidTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_start_calculation: datetime.datetime | None = None,
        max_start_calculation: datetime.datetime | None = None,
        min_end_calculation: datetime.datetime | None = None,
        max_end_calculation: datetime.datetime | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: BidFields | Sequence[BidFields] | None = None,
        group_by: BidFields | Sequence[BidFields] = None,
        query: str | None = None,
        search_properties: BidTextFields | Sequence[BidTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_start_calculation: datetime.datetime | None = None,
        max_start_calculation: datetime.datetime | None = None,
        min_end_calculation: datetime.datetime | None = None,
        max_end_calculation: datetime.datetime | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: BidFields | Sequence[BidFields] | None = None,
        group_by: BidFields | Sequence[BidFields] | None = None,
        query: str | None = None,
        search_property: BidTextFields | Sequence[BidTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_start_calculation: datetime.datetime | None = None,
        max_start_calculation: datetime.datetime | None = None,
        min_end_calculation: datetime.datetime | None = None,
        max_end_calculation: datetime.datetime | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across bids

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            method: The method to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            total: The total to filter on.
            min_start_calculation: The minimum value of the start calculation to filter on.
            max_start_calculation: The maximum value of the start calculation to filter on.
            min_end_calculation: The minimum value of the end calculation to filter on.
            max_end_calculation: The maximum value of the end calculation to filter on.
            market: The market to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bids to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count bids in space `my_space`:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> result = client.bid.aggregate("count", space="my_space")

        """

        filter_ = _create_bid_filter(
            self._view_id,
            name,
            name_prefix,
            method,
            price_area,
            price_area_prefix,
            min_date,
            max_date,
            total,
            min_start_calculation,
            max_start_calculation,
            min_end_calculation,
            max_end_calculation,
            market,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _BID_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: BidFields,
        interval: float,
        query: str | None = None,
        search_property: BidTextFields | Sequence[BidTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_start_calculation: datetime.datetime | None = None,
        max_start_calculation: datetime.datetime | None = None,
        min_end_calculation: datetime.datetime | None = None,
        max_end_calculation: datetime.datetime | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for bids

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            method: The method to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            total: The total to filter on.
            min_start_calculation: The minimum value of the start calculation to filter on.
            max_start_calculation: The maximum value of the start calculation to filter on.
            min_end_calculation: The minimum value of the end calculation to filter on.
            max_end_calculation: The maximum value of the end calculation to filter on.
            market: The market to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bids to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_bid_filter(
            self._view_id,
            name,
            name_prefix,
            method,
            price_area,
            price_area_prefix,
            min_date,
            max_date,
            total,
            min_start_calculation,
            max_start_calculation,
            min_end_calculation,
            max_end_calculation,
            market,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _BID_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        min_date: datetime.date | None = None,
        max_date: datetime.date | None = None,
        total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_start_calculation: datetime.datetime | None = None,
        max_start_calculation: datetime.datetime | None = None,
        min_end_calculation: datetime.datetime | None = None,
        max_end_calculation: datetime.datetime | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> BidList:
        """List/filter bids

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            method: The method to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            min_date: The minimum value of the date to filter on.
            max_date: The maximum value of the date to filter on.
            total: The total to filter on.
            min_start_calculation: The minimum value of the start calculation to filter on.
            max_start_calculation: The maximum value of the start calculation to filter on.
            min_end_calculation: The minimum value of the end calculation to filter on.
            max_end_calculation: The maximum value of the end calculation to filter on.
            market: The market to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bids to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `alerts` or `partials` external ids for the bids. Defaults to True.

        Returns:
            List of requested bids

        Examples:

            List bids and limit to 5:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> bids = client.bid.list(limit=5)

        """
        filter_ = _create_bid_filter(
            self._view_id,
            name,
            name_prefix,
            method,
            price_area,
            price_area_prefix,
            min_date,
            max_date,
            total,
            min_start_calculation,
            max_start_calculation,
            min_end_calculation,
            max_end_calculation,
            market,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_triple=[
                (
                    self.alerts_edge,
                    "alerts",
                    dm.DirectRelationReference("poweropsDayAheadFrontendContractModel", "Bid.alerts"),
                ),
                (
                    self.partials_edge,
                    "partials",
                    dm.DirectRelationReference("poweropsDayAheadFrontendContractModel", "Bid.partials"),
                ),
            ],
        )
