from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes import (
    DomainModelApply,
    SHOPTable,
    SHOPTableApply,
    SHOPTableApplyList,
    SHOPTableFields,
    SHOPTableList,
    SHOPTableTextFields,
)
from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes._shop_table import (
    _SHOPTABLE_PROPERTIES_BY_FIELD,
)

from ._core import DEFAULT_LIMIT_READ, IN_FILTER_LIMIT, Aggregations, TypeAPI


class SHOPTableAlertsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(
        self,
        external_id: str | Sequence[str] | dm.NodeId | list[dm.NodeId],
        space: str = "dayAheadFrontendContractModel",
    ) -> dm.EdgeList:
        """Retrieve one or more alerts edges by id(s) of a shop table.

        Args:
            external_id: External id or list of external ids source shop table.
            space: The space where all the alert edges are located.

        Returns:
            The requested alert edges.

        Examples:

            Retrieve alerts edge by id:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> shop_table = client.shop_table.alerts.retrieve("my_alerts")

        """
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "dayAheadFrontendContractModel", "externalId": "BidTable.alerts"},
        )
        if isinstance(external_id, (str, dm.NodeId)):
            is_shop_tables = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id}
                if isinstance(external_id, str)
                else external_id.dump(camel_case=True, include_instance_type=False),
            )
        else:
            is_shop_tables = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in external_id
                ],
            )
        return self._client.data_modeling.instances.list("edge", limit=-1, filter=f.And(is_edge_type, is_shop_tables))

    def list(
        self,
        shop_table_id: str | list[str] | dm.NodeId | list[dm.NodeId] | None = None,
        limit=DEFAULT_LIMIT_READ,
        space: str = "dayAheadFrontendContractModel",
    ) -> dm.EdgeList:
        """List alerts edges of a shop table.

        Args:
            shop_table_id: ID of the source shop table.
            limit: Maximum number of alert edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            space: The space where all the alert edges are located.

        Returns:
            The requested alert edges.

        Examples:

            List 5 alerts edges connected to "my_shop_table":

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> shop_table = client.shop_table.alerts.list("my_shop_table", limit=5)

        """
        f = dm.filters
        filters = [
            f.Equals(
                ["edge", "type"],
                {"space": "dayAheadFrontendContractModel", "externalId": "BidTable.alerts"},
            )
        ]
        if shop_table_id:
            shop_table_ids = shop_table_id if isinstance(shop_table_id, list) else [shop_table_id]
            is_shop_tables = f.In(
                ["edge", "startNode"],
                [
                    {"space": space, "externalId": ext_id}
                    if isinstance(ext_id, str)
                    else ext_id.dump(camel_case=True, include_instance_type=False)
                    for ext_id in shop_table_ids
                ],
            )
            filters.append(is_shop_tables)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class SHOPTableAPI(TypeAPI[SHOPTable, SHOPTableApply, SHOPTableList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[SHOPTableApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=SHOPTable,
            class_apply_type=SHOPTableApply,
            class_list=SHOPTableList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class
        self.alerts = SHOPTableAlertsAPI(client)

    def apply(
        self, shop_table: SHOPTableApply | Sequence[SHOPTableApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        """Add or update (upsert) shop tables.

        Note: This method iterates through all nodes linked to shop_table and create them including the edges
        between the nodes. For example, if any of `alerts` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            shop_table: Shop table or sequence of shop tables to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new shop_table:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes import SHOPTableApply
                >>> client = DayAheadFrontendContractAPI()
                >>> shop_table = SHOPTableApply(external_id="my_shop_table", ...)
                >>> result = client.shop_table.apply(shop_table)

        """
        if isinstance(shop_table, SHOPTableApply):
            instances = shop_table.to_instances_apply(self._view_by_write_class)
        else:
            instances = SHOPTableApplyList(shop_table).to_instances_apply(self._view_by_write_class)
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
        """Delete one or more shop table.

        Args:
            external_id: External id of the shop table to delete.
            space: The space where all the shop table are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete shop_table by id:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> client.shop_table.delete("my_shop_table")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> SHOPTable:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> SHOPTableList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "dayAheadFrontendContractModel"
    ) -> SHOPTable | SHOPTableList:
        """Retrieve one or more shop tables by id(s).

        Args:
            external_id: External id or list of external ids of the shop tables.
            space: The space where all the shop tables are located.

        Returns:
            The requested shop tables.

        Examples:

            Retrieve shop_table by id:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> shop_table = client.shop_table.retrieve("my_shop_table")

        """
        if isinstance(external_id, str):
            shop_table = self._retrieve((space, external_id))

            alert_edges = self.alerts.retrieve(external_id, space=space)
            shop_table.alerts = [edge.end_node.external_id for edge in alert_edges]

            return shop_table
        else:
            shop_tables = self._retrieve([(space, ext_id) for ext_id in external_id])

            alert_edges = self.alerts.retrieve(shop_tables.as_node_ids())
            self._set_alerts(shop_tables, alert_edges)

            return shop_tables

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
        production_price_pair: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
            production_price_pair: The production price pair to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop tables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `alerts` external ids for the shop tables. Defaults to True.

        Returns:
            Search results shop tables matching the query.

        Examples:

           Search for 'my_shop_table' in all text properties:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> shop_tables = client.shop_table.search('my_shop_table')

        """
        filter_ = _create_filter(
            self._view_id,
            resource_cost,
            resource_cost_prefix,
            asset_type,
            asset_type_prefix,
            asset_id,
            asset_id_prefix,
            production_price_pair,
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
        production_price_pair: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        production_price_pair: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        production_price_pair: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
            production_price_pair: The production price pair to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop tables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `alerts` external ids for the shop tables. Defaults to True.

        Returns:
            Aggregation results.

        Examples:

            Count shop tables in space `my_space`:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> result = client.shop_table.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            resource_cost,
            resource_cost_prefix,
            asset_type,
            asset_type_prefix,
            asset_id,
            asset_id_prefix,
            production_price_pair,
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
        production_price_pair: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
            production_price_pair: The production price pair to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop tables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `alerts` external ids for the shop tables. Defaults to True.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
            resource_cost,
            resource_cost_prefix,
            asset_type,
            asset_type_prefix,
            asset_id,
            asset_id_prefix,
            production_price_pair,
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
        production_price_pair: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
            production_price_pair: The production price pair to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop tables to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `alerts` external ids for the shop tables. Defaults to True.

        Returns:
            List of requested shop tables

        Examples:

            List shop tables and limit to 5:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> shop_tables = client.shop_table.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            resource_cost,
            resource_cost_prefix,
            asset_type,
            asset_type_prefix,
            asset_id,
            asset_id_prefix,
            production_price_pair,
            external_id_prefix,
            space,
            filter,
        )

        shop_tables = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            space_arg = {"space": space} if space else {}
            if len(ids := shop_tables.as_node_ids()) > IN_FILTER_LIMIT:
                alert_edges = self.alerts.list(limit=-1, **space_arg)
            else:
                alert_edges = self.alerts.list(ids, limit=-1)
            self._set_alerts(shop_tables, alert_edges)

        return shop_tables

    @staticmethod
    def _set_alerts(shop_tables: Sequence[SHOPTable], alert_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in alert_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for shop_table in shop_tables:
            node_id = shop_table.id_tuple()
            if node_id in edges_by_start_node:
                shop_table.alerts = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    resource_cost: str | list[str] | None = None,
    resource_cost_prefix: str | None = None,
    asset_type: str | list[str] | None = None,
    asset_type_prefix: str | None = None,
    asset_id: str | list[str] | None = None,
    asset_id_prefix: str | None = None,
    production_price_pair: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if resource_cost and isinstance(resource_cost, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("resourceCost"), value=resource_cost))
    if resource_cost and isinstance(resource_cost, list):
        filters.append(dm.filters.In(view_id.as_property_ref("resourceCost"), values=resource_cost))
    if resource_cost_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("resourceCost"), value=resource_cost_prefix))
    if asset_type and isinstance(asset_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("assetType"), value=asset_type))
    if asset_type and isinstance(asset_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("assetType"), values=asset_type))
    if asset_type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("assetType"), value=asset_type_prefix))
    if asset_id and isinstance(asset_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("asset_id"), value=asset_id))
    if asset_id and isinstance(asset_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("asset_id"), values=asset_id))
    if asset_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("asset_id"), value=asset_id_prefix))
    if production_price_pair and isinstance(production_price_pair, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("productionPricePair"),
                value={"space": "dayAheadFrontendContractModel", "externalId": production_price_pair},
            )
        )
    if production_price_pair and isinstance(production_price_pair, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("productionPricePair"),
                value={"space": production_price_pair[0], "externalId": production_price_pair[1]},
            )
        )
    if production_price_pair and isinstance(production_price_pair, list) and isinstance(production_price_pair[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("productionPricePair"),
                values=[
                    {"space": "dayAheadFrontendContractModel", "externalId": item} for item in production_price_pair
                ],
            )
        )
    if (
        production_price_pair
        and isinstance(production_price_pair, list)
        and isinstance(production_price_pair[0], tuple)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("productionPricePair"),
                values=[{"space": item[0], "externalId": item[1]} for item in production_price_pair],
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
