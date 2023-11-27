from __future__ import annotations

import datetime
from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes import (
    Bid,
    BidApply,
    BidApplyList,
    BidFields,
    BidList,
    BidTextFields,
    DomainModelApply,
)
from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes._bid import _BID_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, IN_FILTER_LIMIT, Aggregations, TypeAPI


class BidAlertsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self,
        external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId],
        space: str = "dayAheadFrontendContractModel",
    ) -> dm.EdgeList:
        """Retrieve one or more alerts edges by id(s) of a bid.

        Args:
            external_id: External id or list of external ids source bid.
            space: The space where all the alert edges are located.

        Returns:
            The requested alert edges.

        Examples:

            Retrieve alerts edge by id:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> bid = client.bid.alerts.retrieve("my_alerts")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "dayAheadFrontendContractModel", "externalId": "Bid.alerts"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_bids = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_bids = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_bids))

    def list(
        self,
        bid_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "dayAheadFrontendContractModel",
    ) -> dm.EdgeList:
        """List alerts edges of a bid.

        Args:
            bid_id: ID of the source bid.
            limit: Maximum number of alert edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the alert edges are located.

        Returns:
            The requested alert edges.

        Examples:

            List 5 alerts edges connected to "my_bid":

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> bid = client.bid.alerts.list("my_bid", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "dayAheadFrontendContractModel", "externalId": "Bid.alerts"},
            )
        ]
        if bid_id:
            bid_ids = bid_id if isinstance(bid_id, list) else [bid_id]
            is_bids = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in bid_ids
                ],
            )
            filters.append(is_bids)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class BidPartialsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self,
        external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId],
        space: str = "dayAheadFrontendContractModel",
    ) -> dm.EdgeList:
        """Retrieve one or more partials edges by id(s) of a bid.

        Args:
            external_id: External id or list of external ids source bid.
            space: The space where all the partial edges are located.

        Returns:
            The requested partial edges.

        Examples:

            Retrieve partials edge by id:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> bid = client.bid.partials.retrieve("my_partials")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "dayAheadFrontendContractModel", "externalId": "Bid.partials"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_bids = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_bids = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_bids))

    def list(
        self,
        bid_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "dayAheadFrontendContractModel",
    ) -> dm.EdgeList:
        """List partials edges of a bid.

        Args:
            bid_id: ID of the source bid.
            limit: Maximum number of partial edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the partial edges are located.

        Returns:
            The requested partial edges.

        Examples:

            List 5 partials edges connected to "my_bid":

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> bid = client.bid.partials.list("my_bid", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "dayAheadFrontendContractModel", "externalId": "Bid.partials"},
            )
        ]
        if bid_id:
            bid_ids = bid_id if isinstance(bid_id, list) else [bid_id]
            is_bids = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in bid_ids
                ],
            )
            filters.append(is_bids)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class BidAPI(TypeAPI[Bid, BidApply, BidList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[BidApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Bid,
            class_apply_type=BidApply,
            class_list=BidList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.alerts = BidAlertsAPI(client)
        self.partials = BidPartialsAPI(client)

    def apply(self, bid: BidApply | Sequence[BidApply], replace: bool = False) -> dm.InstancesApplyResult:
        """Add or update (upsert) bids.

        Note: This method iterates through all nodes linked to bid and create them including the edges
        between the nodes. For example, if any of `alerts` or `partials` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            bid: Bid or sequence of bids to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new bid:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes import BidApply
                >>> client = DayAheadFrontendContractAPI()
                >>> bid = BidApply(external_id="my_bid", ...)
                >>> result = client.bid.apply(bid)

        """
        if isinstance(bid, BidApply):
            instances = bid.to_instances_apply(self._view_by_write_class)
        else:
            instances = BidApplyList(bid).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(
        self, external_id: str | Sequence[str], space: str = "dayAheadFrontendContractModel"
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
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Bid:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> BidList:
        ...

    def retrieve(self, external_id: str | Sequence[str], space: str = "dayAheadFrontendContractModel") -> Bid | BidList:
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
        if isinstance(external_id, str):
            bid = self._retrieve((space, external_id))

            alert_edges = self.alerts.retrieve(external_id, space=space)
            bid.alerts = [edge.end_node.external_id for edge in alert_edges]
            partial_edges = self.partials.retrieve(external_id, space=space)
            bid.partials = [edge.end_node.external_id for edge in partial_edges]

            return bid
        else:
            bids = self._retrieve([(space, ext_id) for ext_id in external_id])

            alert_edges = self.alerts.retrieve(bids.as_node_ids())
            self._set_alerts(bids, alert_edges)
            partial_edges = self.partials.retrieve(bids.as_node_ids())
            self._set_partials(bids, partial_edges)

            return bids

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
            retrieve_edges: Whether to retrieve `alerts` or `partials` external ids for the bids. Defaults to True.

        Returns:
            Search results bids matching the query.

        Examples:

           Search for 'my_bid' in all text properties:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> bids = client.bid.search('my_bid')

        """
        filter_ = _create_filter(
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
            retrieve_edges: Whether to retrieve `alerts` or `partials` external ids for the bids. Defaults to True.

        Returns:
            Aggregation results.

        Examples:

            Count bids in space `my_space`:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> result = client.bid.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
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
            retrieve_edges: Whether to retrieve `alerts` or `partials` external ids for the bids. Defaults to True.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
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
        filter_ = _create_filter(
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

        bids = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            space_arg = {"space": space} if space else {}
            if len(ids := bids.as_node_ids()) > IN_FILTER_LIMIT:
                alert_edges = self.alerts.list(limit=-1, **space_arg)
            else:
                alert_edges = self.alerts.list(ids, limit=-1)
            self._set_alerts(bids, alert_edges)
            if len(ids := bids.as_node_ids()) > IN_FILTER_LIMIT:
                partial_edges = self.partials.list(limit=-1, **space_arg)
            else:
                partial_edges = self.partials.list(ids, limit=-1)
            self._set_partials(bids, partial_edges)

        return bids

    @staticmethod
    def _set_alerts(bids: Sequence[Bid], alert_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in alert_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for bid in bids:
            node_id = bid.id_tuple()
            if node_id in edges_by_start_node:
                bid.alerts = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_partials(bids: Sequence[Bid], partial_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in partial_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for bid in bids:
            node_id = bid.id_tuple()
            if node_id in edges_by_start_node:
                bid.partials = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
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
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if method and isinstance(method, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("method"),
                value={"space": "dayAheadFrontendContractModel", "externalId": method},
            )
        )
    if method and isinstance(method, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("method"), value={"space": method[0], "externalId": method[1]})
        )
    if method and isinstance(method, list) and isinstance(method[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("method"),
                values=[{"space": "dayAheadFrontendContractModel", "externalId": item} for item in method],
            )
        )
    if method and isinstance(method, list) and isinstance(method[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("method"), values=[{"space": item[0], "externalId": item[1]} for item in method]
            )
        )
    if price_area and isinstance(price_area, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("priceArea"), value=price_area))
    if price_area and isinstance(price_area, list):
        filters.append(dm.filters.In(view_id.as_property_ref("priceArea"), values=price_area))
    if price_area_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("priceArea"), value=price_area_prefix))
    if min_date or max_date:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("date"),
                gte=min_date.isoformat() if min_date else None,
                lte=max_date.isoformat() if max_date else None,
            )
        )
    if total and isinstance(total, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("total"), value={"space": "dayAheadFrontendContractModel", "externalId": total}
            )
        )
    if total and isinstance(total, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("total"), value={"space": total[0], "externalId": total[1]})
        )
    if total and isinstance(total, list) and isinstance(total[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("total"),
                values=[{"space": "dayAheadFrontendContractModel", "externalId": item} for item in total],
            )
        )
    if total and isinstance(total, list) and isinstance(total[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("total"), values=[{"space": item[0], "externalId": item[1]} for item in total]
            )
        )
    if min_start_calculation or max_start_calculation:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("startCalculation"),
                gte=min_start_calculation.isoformat(timespec="milliseconds") if min_start_calculation else None,
                lte=max_start_calculation.isoformat(timespec="milliseconds") if max_start_calculation else None,
            )
        )
    if min_end_calculation or max_end_calculation:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("endCalculation"),
                gte=min_end_calculation.isoformat(timespec="milliseconds") if min_end_calculation else None,
                lte=max_end_calculation.isoformat(timespec="milliseconds") if max_end_calculation else None,
            )
        )
    if market and isinstance(market, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("market"),
                value={"space": "dayAheadFrontendContractModel", "externalId": market},
            )
        )
    if market and isinstance(market, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("market"), value={"space": market[0], "externalId": market[1]})
        )
    if market and isinstance(market, list) and isinstance(market[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("market"),
                values=[{"space": "dayAheadFrontendContractModel", "externalId": item} for item in market],
            )
        )
    if market and isinstance(market, list) and isinstance(market[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("market"), values=[{"space": item[0], "externalId": item[1]} for item in market]
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
