from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import (
    DayAheadBid,
    DayAheadBidApply,
    DayAheadBidApplyList,
    DayAheadBidList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class DayAheadBidDateAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "DayAheadBid.date"},
        )
        if isinstance(external_id, str):
            is_day_ahead_bid = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_day_ahead_bid)
            )

        else:
            is_day_ahead_bids = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_day_ahead_bids)
            )

    def list(self, day_ahead_bid_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "DayAheadBid.date"},
        )
        filters.append(is_edge_type)
        if day_ahead_bid_id:
            day_ahead_bid_ids = [day_ahead_bid_id] if isinstance(day_ahead_bid_id, str) else day_ahead_bid_id
            is_day_ahead_bids = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in day_ahead_bid_ids],
            )
            filters.append(is_day_ahead_bids)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class DayAheadBidPriceScenariosAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "DayAheadBid.priceScenarios"},
        )
        if isinstance(external_id, str):
            is_day_ahead_bid = f.Equals(
                ["edge", "startNode"],
                {"space": "power-ops", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_day_ahead_bid)
            )

        else:
            is_day_ahead_bids = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_day_ahead_bids)
            )

    def list(self, day_ahead_bid_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "power-ops", "externalId": "DayAheadBid.priceScenarios"},
        )
        filters.append(is_edge_type)
        if day_ahead_bid_id:
            day_ahead_bid_ids = [day_ahead_bid_id] if isinstance(day_ahead_bid_id, str) else day_ahead_bid_id
            is_day_ahead_bids = f.In(
                ["edge", "startNode"],
                [{"space": "power-ops", "externalId": ext_id} for ext_id in day_ahead_bid_ids],
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
        self.view_id = view_id
        self.date = DayAheadBidDateAPI(client)
        self.price_scenarios = DayAheadBidPriceScenariosAPI(client)

    def apply(
        self, day_ahead_bid: DayAheadBidApply | Sequence[DayAheadBidApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(day_ahead_bid, DayAheadBidApply):
            instances = day_ahead_bid.to_instances_apply()
        else:
            instances = DayAheadBidApplyList(day_ahead_bid).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(DayAheadBidApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(DayAheadBidApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> DayAheadBid:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> DayAheadBidList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> DayAheadBid | DayAheadBidList:
        if isinstance(external_id, str):
            day_ahead_bid = self._retrieve((self.sources.space, external_id))

            date_edges = self.date.retrieve(external_id)
            day_ahead_bid.date = [edge.end_node.external_id for edge in date_edges]
            price_scenario_edges = self.price_scenarios.retrieve(external_id)
            day_ahead_bid.price_scenarios = [edge.end_node.external_id for edge in price_scenario_edges]

            return day_ahead_bid
        else:
            day_ahead_bids = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            date_edges = self.date.retrieve(external_id)
            self._set_date(day_ahead_bids, date_edges)
            price_scenario_edges = self.price_scenarios.retrieve(external_id)
            self._set_price_scenarios(day_ahead_bids, price_scenario_edges)

            return day_ahead_bids

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
            self.view_id,
            name,
            name_prefix,
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
            date_edges = self.date.list(day_ahead_bids.as_external_ids(), limit=-1)
            self._set_date(day_ahead_bids, date_edges)
            price_scenario_edges = self.price_scenarios.list(day_ahead_bids.as_external_ids(), limit=-1)
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
