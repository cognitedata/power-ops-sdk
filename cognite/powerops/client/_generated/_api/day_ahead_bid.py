from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    DayAheadBid,
    DayAheadBidApply,
    DayAheadBidApplyList,
    DayAheadBidFields,
    DayAheadBidList,
    DayAheadBidTextFields,
)
from cognite.powerops.client._generated.data_classes._day_ahead_bid import _DAYAHEADBID_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, IN_FILTER_LIMIT, Aggregations, TypeAPI


class DayAheadBidDateAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="power-ops") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "DayAheadBid.date"},
        )
        if isinstance(external_id, str):
            is_day_ahead_bid = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_day_ahead_bid)
            )

        else:
            is_day_ahead_bids = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_day_ahead_bids)
            )

    def list(
        self, day_ahead_bid_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="power-ops"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "DayAheadBid.date"},
        )
        filters.append(is_edge_type)
        if day_ahead_bid_id:
            day_ahead_bid_ids = [day_ahead_bid_id] if isinstance(day_ahead_bid_id, str) else day_ahead_bid_id
            is_day_ahead_bids = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in day_ahead_bid_ids],
            )
            filters.append(is_day_ahead_bids)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class DayAheadBidPriceScenariosAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str], space="power-ops") -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "DayAheadBid.priceScenarios"},
        )
        if isinstance(external_id, str):
            is_day_ahead_bid = f.Equals(
                ["edge", "startNode"],
                {"space": space, "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_day_ahead_bid)
            )

        else:
            is_day_ahead_bids = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_day_ahead_bids)
            )

    def list(
        self, day_ahead_bid_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ, space="power-ops"
    ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": space, "externalId": "DayAheadBid.priceScenarios"},
        )
        filters.append(is_edge_type)
        if day_ahead_bid_id:
            day_ahead_bid_ids = [day_ahead_bid_id] if isinstance(day_ahead_bid_id, str) else day_ahead_bid_id
            is_day_ahead_bids = f.In(
                ["edge", "startNode"],
                [{"space": space, "externalId": ext_id} for ext_id in day_ahead_bid_ids],
            )
            filters.append(is_day_ahead_bids)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class DayAheadBidAPI(TypeAPI[DayAheadBid, DayAheadBidApply, DayAheadBidList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=DayAheadBid,
            class_apply_type=DayAheadBidApply,
            class_list=DayAheadBidList,
        )
        self._view_id = view_id
        self.date = DayAheadBidDateAPI(client)
        self.price_scenarios = DayAheadBidPriceScenariosAPI(client)

    def apply(
        self, day_ahead_bid: DayAheadBidApply | Sequence[DayAheadBidApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(day_ahead_bid, DayAheadBidApply):
            instances = day_ahead_bid.to_instances_apply()
        else:
            instances = DayAheadBidApplyList(day_ahead_bid).to_instances_apply()
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space="power-ops") -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> DayAheadBid:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> DayAheadBidList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> DayAheadBid | DayAheadBidList:
        if isinstance(external_id, str):
            day_ahead_bid = self._retrieve((self._sources.space, external_id))

            date_edges = self.date.retrieve(external_id)
            day_ahead_bid.date = [edge.end_node.external_id for edge in date_edges]
            price_scenario_edges = self.price_scenarios.retrieve(external_id)
            day_ahead_bid.price_scenarios = [edge.end_node.external_id for edge in price_scenario_edges]

            return day_ahead_bid
        else:
            day_ahead_bids = self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

            date_edges = self.date.retrieve(external_id)
            self._set_date(day_ahead_bids, date_edges)
            price_scenario_edges = self.price_scenarios.retrieve(external_id)
            self._set_price_scenarios(day_ahead_bids, price_scenario_edges)

            return day_ahead_bids

    def search(
        self,
        query: str,
        properties: DayAheadBidTextFields | Sequence[DayAheadBidTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        is_default_config_for_price_area: bool | None = None,
        main_scenario: str | list[str] | None = None,
        main_scenario_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        watercourse: str | list[str] | None = None,
        watercourse_prefix: str | None = None,
        no_shop: bool | None = None,
        bid_process_configuration_name: str | list[str] | None = None,
        bid_process_configuration_name_prefix: str | None = None,
        bid_matrix_generator_config_external_id: str | list[str] | None = None,
        bid_matrix_generator_config_external_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> DayAheadBidList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            market,
            is_default_config_for_price_area,
            main_scenario,
            main_scenario_prefix,
            price_area,
            price_area_prefix,
            watercourse,
            watercourse_prefix,
            no_shop,
            bid_process_configuration_name,
            bid_process_configuration_name_prefix,
            bid_matrix_generator_config_external_id,
            bid_matrix_generator_config_external_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _DAYAHEADBID_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: DayAheadBidFields | Sequence[DayAheadBidFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: DayAheadBidTextFields | Sequence[DayAheadBidTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        is_default_config_for_price_area: bool | None = None,
        main_scenario: str | list[str] | None = None,
        main_scenario_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        watercourse: str | list[str] | None = None,
        watercourse_prefix: str | None = None,
        no_shop: bool | None = None,
        bid_process_configuration_name: str | list[str] | None = None,
        bid_process_configuration_name_prefix: str | None = None,
        bid_matrix_generator_config_external_id: str | list[str] | None = None,
        bid_matrix_generator_config_external_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
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
        property: DayAheadBidFields | Sequence[DayAheadBidFields] | None = None,
        group_by: DayAheadBidFields | Sequence[DayAheadBidFields] = None,
        query: str | None = None,
        search_properties: DayAheadBidTextFields | Sequence[DayAheadBidTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        is_default_config_for_price_area: bool | None = None,
        main_scenario: str | list[str] | None = None,
        main_scenario_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        watercourse: str | list[str] | None = None,
        watercourse_prefix: str | None = None,
        no_shop: bool | None = None,
        bid_process_configuration_name: str | list[str] | None = None,
        bid_process_configuration_name_prefix: str | None = None,
        bid_matrix_generator_config_external_id: str | list[str] | None = None,
        bid_matrix_generator_config_external_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
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
        property: DayAheadBidFields | Sequence[DayAheadBidFields] | None = None,
        group_by: DayAheadBidFields | Sequence[DayAheadBidFields] | None = None,
        query: str | None = None,
        search_property: DayAheadBidTextFields | Sequence[DayAheadBidTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        is_default_config_for_price_area: bool | None = None,
        main_scenario: str | list[str] | None = None,
        main_scenario_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        watercourse: str | list[str] | None = None,
        watercourse_prefix: str | None = None,
        no_shop: bool | None = None,
        bid_process_configuration_name: str | list[str] | None = None,
        bid_process_configuration_name_prefix: str | None = None,
        bid_matrix_generator_config_external_id: str | list[str] | None = None,
        bid_matrix_generator_config_external_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            market,
            is_default_config_for_price_area,
            main_scenario,
            main_scenario_prefix,
            price_area,
            price_area_prefix,
            watercourse,
            watercourse_prefix,
            no_shop,
            bid_process_configuration_name,
            bid_process_configuration_name_prefix,
            bid_matrix_generator_config_external_id,
            bid_matrix_generator_config_external_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _DAYAHEADBID_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: DayAheadBidFields,
        interval: float,
        query: str | None = None,
        search_property: DayAheadBidTextFields | Sequence[DayAheadBidTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        is_default_config_for_price_area: bool | None = None,
        main_scenario: str | list[str] | None = None,
        main_scenario_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        watercourse: str | list[str] | None = None,
        watercourse_prefix: str | None = None,
        no_shop: bool | None = None,
        bid_process_configuration_name: str | list[str] | None = None,
        bid_process_configuration_name_prefix: str | None = None,
        bid_matrix_generator_config_external_id: str | list[str] | None = None,
        bid_matrix_generator_config_external_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            market,
            is_default_config_for_price_area,
            main_scenario,
            main_scenario_prefix,
            price_area,
            price_area_prefix,
            watercourse,
            watercourse_prefix,
            no_shop,
            bid_process_configuration_name,
            bid_process_configuration_name_prefix,
            bid_matrix_generator_config_external_id,
            bid_matrix_generator_config_external_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _DAYAHEADBID_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        is_default_config_for_price_area: bool | None = None,
        main_scenario: str | list[str] | None = None,
        main_scenario_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        watercourse: str | list[str] | None = None,
        watercourse_prefix: str | None = None,
        no_shop: bool | None = None,
        bid_process_configuration_name: str | list[str] | None = None,
        bid_process_configuration_name_prefix: str | None = None,
        bid_matrix_generator_config_external_id: str | list[str] | None = None,
        bid_matrix_generator_config_external_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> DayAheadBidList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            market,
            is_default_config_for_price_area,
            main_scenario,
            main_scenario_prefix,
            price_area,
            price_area_prefix,
            watercourse,
            watercourse_prefix,
            no_shop,
            bid_process_configuration_name,
            bid_process_configuration_name_prefix,
            bid_matrix_generator_config_external_id,
            bid_matrix_generator_config_external_id_prefix,
            external_id_prefix,
            filter,
        )

        day_ahead_bids = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            if len(external_ids := day_ahead_bids.as_external_ids()) > IN_FILTER_LIMIT:
                date_edges = self.date.list(limit=-1)
            else:
                date_edges = self.date.list(external_ids, limit=-1)
            self._set_date(day_ahead_bids, date_edges)
            if len(external_ids := day_ahead_bids.as_external_ids()) > IN_FILTER_LIMIT:
                price_scenario_edges = self.price_scenarios.list(limit=-1)
            else:
                price_scenario_edges = self.price_scenarios.list(external_ids, limit=-1)
            self._set_price_scenarios(day_ahead_bids, price_scenario_edges)

        return day_ahead_bids

    @staticmethod
    def _set_date(day_ahead_bids: Sequence[DayAheadBid], date_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in date_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for day_ahead_bid in day_ahead_bids:
            node_id = day_ahead_bid.id_tuple()
            if node_id in edges_by_start_node:
                day_ahead_bid.date = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]

    @staticmethod
    def _set_price_scenarios(day_ahead_bids: Sequence[DayAheadBid], price_scenario_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in price_scenario_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for day_ahead_bid in day_ahead_bids:
            node_id = day_ahead_bid.id_tuple()
            if node_id in edges_by_start_node:
                day_ahead_bid.price_scenarios = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    market: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    is_default_config_for_price_area: bool | None = None,
    main_scenario: str | list[str] | None = None,
    main_scenario_prefix: str | None = None,
    price_area: str | list[str] | None = None,
    price_area_prefix: str | None = None,
    watercourse: str | list[str] | None = None,
    watercourse_prefix: str | None = None,
    no_shop: bool | None = None,
    bid_process_configuration_name: str | list[str] | None = None,
    bid_process_configuration_name_prefix: str | None = None,
    bid_matrix_generator_config_external_id: str | list[str] | None = None,
    bid_matrix_generator_config_external_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if market and isinstance(market, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("market"), value={"space": "power-ops", "externalId": market})
        )
    if market and isinstance(market, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("market"), value={"space": market[0], "externalId": market[1]})
        )
    if market and isinstance(market, list) and isinstance(market[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("market"),
                values=[{"space": "power-ops", "externalId": item} for item in market],
            )
        )
    if market and isinstance(market, list) and isinstance(market[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("market"), values=[{"space": item[0], "externalId": item[1]} for item in market]
            )
        )
    if is_default_config_for_price_area and isinstance(is_default_config_for_price_area, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("isDefaultConfigForPriceArea"), value=is_default_config_for_price_area
            )
        )
    if main_scenario and isinstance(main_scenario, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("mainScenario"), value=main_scenario))
    if main_scenario and isinstance(main_scenario, list):
        filters.append(dm.filters.In(view_id.as_property_ref("mainScenario"), values=main_scenario))
    if main_scenario_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("mainScenario"), value=main_scenario_prefix))
    if price_area and isinstance(price_area, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("priceArea"), value=price_area))
    if price_area and isinstance(price_area, list):
        filters.append(dm.filters.In(view_id.as_property_ref("priceArea"), values=price_area))
    if price_area_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("priceArea"), value=price_area_prefix))
    if watercourse and isinstance(watercourse, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("watercourse"), value=watercourse))
    if watercourse and isinstance(watercourse, list):
        filters.append(dm.filters.In(view_id.as_property_ref("watercourse"), values=watercourse))
    if watercourse_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("watercourse"), value=watercourse_prefix))
    if no_shop and isinstance(no_shop, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("noShop"), value=no_shop))
    if bid_process_configuration_name and isinstance(bid_process_configuration_name, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("bidProcessConfigurationName"), value=bid_process_configuration_name
            )
        )
    if bid_process_configuration_name and isinstance(bid_process_configuration_name, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("bidProcessConfigurationName"), values=bid_process_configuration_name)
        )
    if bid_process_configuration_name_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("bidProcessConfigurationName"), value=bid_process_configuration_name_prefix
            )
        )
    if bid_matrix_generator_config_external_id and isinstance(bid_matrix_generator_config_external_id, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("bidMatrixGeneratorConfigExternalId"),
                value=bid_matrix_generator_config_external_id,
            )
        )
    if bid_matrix_generator_config_external_id and isinstance(bid_matrix_generator_config_external_id, list):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("bidMatrixGeneratorConfigExternalId"),
                values=bid_matrix_generator_config_external_id,
            )
        )
    if bid_matrix_generator_config_external_id_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("bidMatrixGeneratorConfigExternalId"),
                value=bid_matrix_generator_config_external_id_prefix,
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
