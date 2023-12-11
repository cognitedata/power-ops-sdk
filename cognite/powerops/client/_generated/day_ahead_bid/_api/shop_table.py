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
    SHOPTable,
    SHOPTableApply,
    SHOPTableFields,
    SHOPTableList,
    SHOPTableApplyList,
    SHOPTableTextFields,
)
from cognite.powerops.client._generated.day_ahead_bid.data_classes._shop_table import (
    _SHOPTABLE_PROPERTIES_BY_FIELD,
    _create_shop_table_filter,
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
from .shop_table_alerts import SHOPTableAlertsAPI
from .shop_table_production import SHOPTableProductionAPI
from .shop_table_price import SHOPTablePriceAPI
from .shop_table_query import SHOPTableQueryAPI


class SHOPTableAPI(NodeAPI[SHOPTable, SHOPTableApply, SHOPTableList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[SHOPTableApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=SHOPTable,
            class_apply_type=SHOPTableApply,
            class_list=SHOPTableList,
            class_apply_list=SHOPTableApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.alerts_edge = SHOPTableAlertsAPI(client)
        self.production = SHOPTableProductionAPI(client, view_id)
        self.price = SHOPTablePriceAPI(client, view_id)

    def __call__(
        self,
        resource_cost: str | list[str] | None = None,
        resource_cost_prefix: str | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> SHOPTableQueryAPI[SHOPTableList]:
        """Query starting at shop tables.

        Args:
            resource_cost: The resource cost to filter on.
            resource_cost_prefix: The prefix of the resource cost to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            asset_id: The asset id to filter on.
            asset_id_prefix: The prefix of the asset id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop tables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for shop tables.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_shop_table_filter(
            self._view_id,
            resource_cost,
            resource_cost_prefix,
            asset_type,
            asset_type_prefix,
            asset_id,
            asset_id_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(SHOPTableList)
        return SHOPTableQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self, shop_table: SHOPTableApply | Sequence[SHOPTableApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) shop tables.

        Note: This method iterates through all nodes and timeseries linked to shop_table and creates them including the edges
        between the nodes. For example, if any of `alerts` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            shop_table: Shop table or sequence of shop tables to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new shop_table:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> from cognite.powerops.client._generated.day_ahead_bid.data_classes import SHOPTableApply
                >>> client = DayAheadBidAPI()
                >>> shop_table = SHOPTableApply(external_id="my_shop_table", ...)
                >>> result = client.shop_table.apply(shop_table)

        """
        return self._apply(shop_table, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more shop table.

        Args:
            external_id: External id of the shop table to delete.
            space: The space where all the shop table are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete shop_table by id:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> client.shop_table.delete("my_shop_table")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> SHOPTable | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> SHOPTableList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> SHOPTable | SHOPTableList | None:
        """Retrieve one or more shop tables by id(s).

        Args:
            external_id: External id or list of external ids of the shop tables.
            space: The space where all the shop tables are located.

        Returns:
            The requested shop tables.

        Examples:

            Retrieve shop_table by id:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> shop_table = client.shop_table.retrieve("my_shop_table")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_triple=[
                (self.alerts_edge, "alerts", dm.DirectRelationReference("power-ops-types", "calculationIssue")),
            ],
        )

    def search(
        self,
        query: str,
        properties: SHOPTableTextFields | Sequence[SHOPTableTextFields] | None = None,
        resource_cost: str | list[str] | None = None,
        resource_cost_prefix: str | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> SHOPTableList:
        """Search shop tables

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            resource_cost: The resource cost to filter on.
            resource_cost_prefix: The prefix of the resource cost to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            asset_id: The asset id to filter on.
            asset_id_prefix: The prefix of the asset id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop tables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results shop tables matching the query.

        Examples:

           Search for 'my_shop_table' in all text properties:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> shop_tables = client.shop_table.search('my_shop_table')

        """
        filter_ = _create_shop_table_filter(
            self._view_id,
            resource_cost,
            resource_cost_prefix,
            asset_type,
            asset_type_prefix,
            asset_id,
            asset_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _SHOPTABLE_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: SHOPTableFields | Sequence[SHOPTableFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: SHOPTableTextFields | Sequence[SHOPTableTextFields] | None = None,
        resource_cost: str | list[str] | None = None,
        resource_cost_prefix: str | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
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
        property: SHOPTableFields | Sequence[SHOPTableFields] | None = None,
        group_by: SHOPTableFields | Sequence[SHOPTableFields] = None,
        query: str | None = None,
        search_properties: SHOPTableTextFields | Sequence[SHOPTableTextFields] | None = None,
        resource_cost: str | list[str] | None = None,
        resource_cost_prefix: str | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
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
        property: SHOPTableFields | Sequence[SHOPTableFields] | None = None,
        group_by: SHOPTableFields | Sequence[SHOPTableFields] | None = None,
        query: str | None = None,
        search_property: SHOPTableTextFields | Sequence[SHOPTableTextFields] | None = None,
        resource_cost: str | list[str] | None = None,
        resource_cost_prefix: str | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across shop tables

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
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop tables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count shop tables in space `my_space`:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> result = client.shop_table.aggregate("count", space="my_space")

        """

        filter_ = _create_shop_table_filter(
            self._view_id,
            resource_cost,
            resource_cost_prefix,
            asset_type,
            asset_type_prefix,
            asset_id,
            asset_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _SHOPTABLE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: SHOPTableFields,
        interval: float,
        query: str | None = None,
        search_property: SHOPTableTextFields | Sequence[SHOPTableTextFields] | None = None,
        resource_cost: str | list[str] | None = None,
        resource_cost_prefix: str | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for shop tables

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
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop tables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_shop_table_filter(
            self._view_id,
            resource_cost,
            resource_cost_prefix,
            asset_type,
            asset_type_prefix,
            asset_id,
            asset_id_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _SHOPTABLE_PROPERTIES_BY_FIELD,
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
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> SHOPTableList:
        """List/filter shop tables

        Args:
            resource_cost: The resource cost to filter on.
            resource_cost_prefix: The prefix of the resource cost to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            asset_id: The asset id to filter on.
            asset_id_prefix: The prefix of the asset id to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop tables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `alerts` external ids for the shop tables. Defaults to True.

        Returns:
            List of requested shop tables

        Examples:

            List shop tables and limit to 5:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> shop_tables = client.shop_table.list(limit=5)

        """
        filter_ = _create_shop_table_filter(
            self._view_id,
            resource_cost,
            resource_cost_prefix,
            asset_type,
            asset_type_prefix,
            asset_id,
            asset_id_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_triple=[
                (self.alerts_edge, "alerts", dm.DirectRelationReference("power-ops-types", "calculationIssue")),
            ],
        )
