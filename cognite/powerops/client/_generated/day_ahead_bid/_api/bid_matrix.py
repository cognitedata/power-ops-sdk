from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.day_ahead_bid.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.day_ahead_bid.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    BidMatrix,
    BidMatrixApply,
    BidMatrixFields,
    BidMatrixList,
    BidMatrixApplyList,
    BidMatrixTextFields,
)
from cognite.powerops.client._generated.day_ahead_bid.data_classes._bid_matrix import (
    _BIDMATRIX_PROPERTIES_BY_FIELD,
    _create_bid_matrix_filter,
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
from .bid_matrix_alerts import BidMatrixAlertsAPI
from .bid_matrix_query import BidMatrixQueryAPI


class BidMatrixAPI(NodeAPI[BidMatrix, BidMatrixApply, BidMatrixList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[BidMatrixApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BidMatrix,
            class_apply_type=BidMatrixApply,
            class_list=BidMatrixList,
            class_apply_list=BidMatrixApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.alerts_edge = BidMatrixAlertsAPI(client)

    def __call__(
        self,
        resource_cost: str | list[str] | None = None,
        resource_cost_prefix: str | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> BidMatrixQueryAPI[BidMatrixList]:
        """Query starting at bid matrixes.

        Args:
            resource_cost: The resource cost to filter on.
            resource_cost_prefix: The prefix of the resource cost to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            asset_id: The asset id to filter on.
            asset_id_prefix: The prefix of the asset id to filter on.
            method: The method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid matrixes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for bid matrixes.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_bid_matrix_filter(
            self._view_id,
            resource_cost,
            resource_cost_prefix,
            asset_type,
            asset_type_prefix,
            asset_id,
            asset_id_prefix,
            method,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(BidMatrixList)
        return BidMatrixQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self, bid_matrix: BidMatrixApply | Sequence[BidMatrixApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) bid matrixes.

        Note: This method iterates through all nodes and timeseries linked to bid_matrix and creates them including the edges
        between the nodes. For example, if any of `alerts` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            bid_matrix: Bid matrix or sequence of bid matrixes to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new bid_matrix:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> from cognite.powerops.client._generated.day_ahead_bid.data_classes import BidMatrixApply
                >>> client = DayAheadBidAPI()
                >>> bid_matrix = BidMatrixApply(external_id="my_bid_matrix", ...)
                >>> result = client.bid_matrix.apply(bid_matrix)

        """
        return self._apply(bid_matrix, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more bid matrix.

        Args:
            external_id: External id of the bid matrix to delete.
            space: The space where all the bid matrix are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete bid_matrix by id:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> client.bid_matrix.delete("my_bid_matrix")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> BidMatrix | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> BidMatrixList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> BidMatrix | BidMatrixList | None:
        """Retrieve one or more bid matrixes by id(s).

        Args:
            external_id: External id or list of external ids of the bid matrixes.
            space: The space where all the bid matrixes are located.

        Returns:
            The requested bid matrixes.

        Examples:

            Retrieve bid_matrix by id:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> bid_matrix = client.bid_matrix.retrieve("my_bid_matrix")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_quad=[
                (
                    self.alerts_edge,
                    "alerts",
                    dm.DirectRelationReference("power-ops-types", "calculationIssue"),
                    "outwards",
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: BidMatrixTextFields | Sequence[BidMatrixTextFields] | None = None,
        resource_cost: str | list[str] | None = None,
        resource_cost_prefix: str | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidMatrixList:
        """Search bid matrixes

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            resource_cost: The resource cost to filter on.
            resource_cost_prefix: The prefix of the resource cost to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            asset_id: The asset id to filter on.
            asset_id_prefix: The prefix of the asset id to filter on.
            method: The method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid matrixes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results bid matrixes matching the query.

        Examples:

           Search for 'my_bid_matrix' in all text properties:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> bid_matrixes = client.bid_matrix.search('my_bid_matrix')

        """
        filter_ = _create_bid_matrix_filter(
            self._view_id,
            resource_cost,
            resource_cost_prefix,
            asset_type,
            asset_type_prefix,
            asset_id,
            asset_id_prefix,
            method,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _BIDMATRIX_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: BidMatrixFields | Sequence[BidMatrixFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: BidMatrixTextFields | Sequence[BidMatrixTextFields] | None = None,
        resource_cost: str | list[str] | None = None,
        resource_cost_prefix: str | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: BidMatrixFields | Sequence[BidMatrixFields] | None = None,
        group_by: BidMatrixFields | Sequence[BidMatrixFields] = None,
        query: str | None = None,
        search_properties: BidMatrixTextFields | Sequence[BidMatrixTextFields] | None = None,
        resource_cost: str | list[str] | None = None,
        resource_cost_prefix: str | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: BidMatrixFields | Sequence[BidMatrixFields] | None = None,
        group_by: BidMatrixFields | Sequence[BidMatrixFields] | None = None,
        query: str | None = None,
        search_property: BidMatrixTextFields | Sequence[BidMatrixTextFields] | None = None,
        resource_cost: str | list[str] | None = None,
        resource_cost_prefix: str | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across bid matrixes

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            resource_cost: The resource cost to filter on.
            resource_cost_prefix: The prefix of the resource cost to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            asset_id: The asset id to filter on.
            asset_id_prefix: The prefix of the asset id to filter on.
            method: The method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid matrixes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count bid matrixes in space `my_space`:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> result = client.bid_matrix.aggregate("count", space="my_space")

        """

        filter_ = _create_bid_matrix_filter(
            self._view_id,
            resource_cost,
            resource_cost_prefix,
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
            _BIDMATRIX_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: BidMatrixFields,
        interval: float,
        query: str | None = None,
        search_property: BidMatrixTextFields | Sequence[BidMatrixTextFields] | None = None,
        resource_cost: str | list[str] | None = None,
        resource_cost_prefix: str | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for bid matrixes

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            resource_cost: The resource cost to filter on.
            resource_cost_prefix: The prefix of the resource cost to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            asset_id: The asset id to filter on.
            asset_id_prefix: The prefix of the asset id to filter on.
            method: The method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid matrixes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_bid_matrix_filter(
            self._view_id,
            resource_cost,
            resource_cost_prefix,
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
            _BIDMATRIX_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        resource_cost: str | list[str] | None = None,
        resource_cost_prefix: str | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> BidMatrixList:
        """List/filter bid matrixes

        Args:
            resource_cost: The resource cost to filter on.
            resource_cost_prefix: The prefix of the resource cost to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            asset_id: The asset id to filter on.
            asset_id_prefix: The prefix of the asset id to filter on.
            method: The method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid matrixes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `alerts` external ids for the bid matrixes. Defaults to True.

        Returns:
            List of requested bid matrixes

        Examples:

            List bid matrixes and limit to 5:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> bid_matrixes = client.bid_matrix.list(limit=5)

        """
        filter_ = _create_bid_matrix_filter(
            self._view_id,
            resource_cost,
            resource_cost_prefix,
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
            edge_api_name_type_direction_quad=[
                (
                    self.alerts_edge,
                    "alerts",
                    dm.DirectRelationReference("power-ops-types", "calculationIssue"),
                    "outwards",
                ),
            ],
        )
