from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    RKOMBidCombination,
    RKOMBidCombinationApply,
    RKOMBidCombinationApplyList,
    RKOMBidCombinationFields,
    RKOMBidCombinationList,
    RKOMBidCombinationTextFields,
)
from cognite.powerops.client._generated.data_classes._rkom_bid_combination import (
    _RKOMBIDCOMBINATION_PROPERTIES_BY_FIELD,
)

from ._core import DEFAULT_LIMIT_READ, Aggregations, TypeAPI


class RKOMBidCombinationAPI(TypeAPI[RKOMBidCombination, RKOMBidCombinationApply, RKOMBidCombinationList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=RKOMBidCombination,
            class_apply_type=RKOMBidCombinationApply,
            class_list=RKOMBidCombinationList,
        )
        self._view_id = view_id

    def apply(
        self, rkom_bid_combination: RKOMBidCombinationApply | Sequence[RKOMBidCombinationApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(rkom_bid_combination, RKOMBidCombinationApply):
            instances = rkom_bid_combination.to_instances_apply()
        else:
            instances = RKOMBidCombinationApplyList(rkom_bid_combination).to_instances_apply()
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
    def retrieve(self, external_id: str) -> RKOMBidCombination:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> RKOMBidCombinationList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> RKOMBidCombination | RKOMBidCombinationList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: RKOMBidCombinationTextFields | Sequence[RKOMBidCombinationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        auction: str | list[str] | None = None,
        auction_prefix: str | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> RKOMBidCombinationList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            auction,
            auction_prefix,
            bid,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _RKOMBIDCOMBINATION_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: RKOMBidCombinationFields | Sequence[RKOMBidCombinationFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: RKOMBidCombinationTextFields | Sequence[RKOMBidCombinationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        auction: str | list[str] | None = None,
        auction_prefix: str | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: RKOMBidCombinationFields | Sequence[RKOMBidCombinationFields] | None = None,
        group_by: RKOMBidCombinationFields | Sequence[RKOMBidCombinationFields] = None,
        query: str | None = None,
        search_properties: RKOMBidCombinationTextFields | Sequence[RKOMBidCombinationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        auction: str | list[str] | None = None,
        auction_prefix: str | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: RKOMBidCombinationFields | Sequence[RKOMBidCombinationFields] | None = None,
        group_by: RKOMBidCombinationFields | Sequence[RKOMBidCombinationFields] | None = None,
        query: str | None = None,
        search_property: RKOMBidCombinationTextFields | Sequence[RKOMBidCombinationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        auction: str | list[str] | None = None,
        auction_prefix: str | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            auction,
            auction_prefix,
            bid,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _RKOMBIDCOMBINATION_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: RKOMBidCombinationFields,
        interval: float,
        query: str | None = None,
        search_property: RKOMBidCombinationTextFields | Sequence[RKOMBidCombinationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        auction: str | list[str] | None = None,
        auction_prefix: str | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            auction,
            auction_prefix,
            bid,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _RKOMBIDCOMBINATION_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        auction: str | list[str] | None = None,
        auction_prefix: str | None = None,
        bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> RKOMBidCombinationList:
        filter_ = _create_filter(
            self._view_id,
            name,
            name_prefix,
            auction,
            auction_prefix,
            bid,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    auction: str | list[str] | None = None,
    auction_prefix: str | None = None,
    bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if auction and isinstance(auction, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("auction"), value=auction))
    if auction and isinstance(auction, list):
        filters.append(dm.filters.In(view_id.as_property_ref("auction"), values=auction))
    if auction_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("auction"), value=auction_prefix))
    if bid and isinstance(bid, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("bid"), value={"space": "power-ops", "externalId": bid})
        )
    if bid and isinstance(bid, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("bid"), value={"space": bid[0], "externalId": bid[1]}))
    if bid and isinstance(bid, list) and isinstance(bid[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("bid"), values=[{"space": "power-ops", "externalId": item} for item in bid]
            )
        )
    if bid and isinstance(bid, list) and isinstance(bid[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("bid"), values=[{"space": item[0], "externalId": item[1]} for item in bid]
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
