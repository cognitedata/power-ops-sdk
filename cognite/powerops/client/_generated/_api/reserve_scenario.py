from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    ReserveScenario,
    ReserveScenarioApply,
    ReserveScenarioApplyList,
    ReserveScenarioFields,
    ReserveScenarioList,
    ReserveScenarioTextFields,
)
from cognite.powerops.client._generated.data_classes._reserve_scenario import _RESERVESCENARIO_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, Aggregations, TypeAPI


class ReserveScenarioAPI(TypeAPI[ReserveScenario, ReserveScenarioApply, ReserveScenarioList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ReserveScenario,
            class_apply_type=ReserveScenarioApply,
            class_list=ReserveScenarioList,
        )
        self._view_id = view_id

    def apply(
        self, reserve_scenario: ReserveScenarioApply | Sequence[ReserveScenarioApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(reserve_scenario, ReserveScenarioApply):
            instances = reserve_scenario.to_instances_apply()
        else:
            instances = ReserveScenarioApplyList(reserve_scenario).to_instances_apply()
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
    def retrieve(self, external_id: str) -> ReserveScenario:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ReserveScenarioList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> ReserveScenario | ReserveScenarioList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: ReserveScenarioTextFields | Sequence[ReserveScenarioTextFields] | None = None,
        min_volume: int | None = None,
        max_volume: int | None = None,
        auction: str | list[str] | None = None,
        auction_prefix: str | None = None,
        product: str | list[str] | None = None,
        product_prefix: str | None = None,
        block: str | list[str] | None = None,
        block_prefix: str | None = None,
        reserve_group: str | list[str] | None = None,
        reserve_group_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ReserveScenarioList:
        filter_ = _create_filter(
            self._view_id,
            min_volume,
            max_volume,
            auction,
            auction_prefix,
            product,
            product_prefix,
            block,
            block_prefix,
            reserve_group,
            reserve_group_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _RESERVESCENARIO_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ReserveScenarioFields | Sequence[ReserveScenarioFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: ReserveScenarioTextFields | Sequence[ReserveScenarioTextFields] | None = None,
        min_volume: int | None = None,
        max_volume: int | None = None,
        auction: str | list[str] | None = None,
        auction_prefix: str | None = None,
        product: str | list[str] | None = None,
        product_prefix: str | None = None,
        block: str | list[str] | None = None,
        block_prefix: str | None = None,
        reserve_group: str | list[str] | None = None,
        reserve_group_prefix: str | None = None,
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
        property: ReserveScenarioFields | Sequence[ReserveScenarioFields] | None = None,
        group_by: ReserveScenarioFields | Sequence[ReserveScenarioFields] = None,
        query: str | None = None,
        search_properties: ReserveScenarioTextFields | Sequence[ReserveScenarioTextFields] | None = None,
        min_volume: int | None = None,
        max_volume: int | None = None,
        auction: str | list[str] | None = None,
        auction_prefix: str | None = None,
        product: str | list[str] | None = None,
        product_prefix: str | None = None,
        block: str | list[str] | None = None,
        block_prefix: str | None = None,
        reserve_group: str | list[str] | None = None,
        reserve_group_prefix: str | None = None,
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
        property: ReserveScenarioFields | Sequence[ReserveScenarioFields] | None = None,
        group_by: ReserveScenarioFields | Sequence[ReserveScenarioFields] | None = None,
        query: str | None = None,
        search_property: ReserveScenarioTextFields | Sequence[ReserveScenarioTextFields] | None = None,
        min_volume: int | None = None,
        max_volume: int | None = None,
        auction: str | list[str] | None = None,
        auction_prefix: str | None = None,
        product: str | list[str] | None = None,
        product_prefix: str | None = None,
        block: str | list[str] | None = None,
        block_prefix: str | None = None,
        reserve_group: str | list[str] | None = None,
        reserve_group_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            min_volume,
            max_volume,
            auction,
            auction_prefix,
            product,
            product_prefix,
            block,
            block_prefix,
            reserve_group,
            reserve_group_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _RESERVESCENARIO_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ReserveScenarioFields,
        interval: float,
        query: str | None = None,
        search_property: ReserveScenarioTextFields | Sequence[ReserveScenarioTextFields] | None = None,
        min_volume: int | None = None,
        max_volume: int | None = None,
        auction: str | list[str] | None = None,
        auction_prefix: str | None = None,
        product: str | list[str] | None = None,
        product_prefix: str | None = None,
        block: str | list[str] | None = None,
        block_prefix: str | None = None,
        reserve_group: str | list[str] | None = None,
        reserve_group_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            min_volume,
            max_volume,
            auction,
            auction_prefix,
            product,
            product_prefix,
            block,
            block_prefix,
            reserve_group,
            reserve_group_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _RESERVESCENARIO_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        min_volume: int | None = None,
        max_volume: int | None = None,
        auction: str | list[str] | None = None,
        auction_prefix: str | None = None,
        product: str | list[str] | None = None,
        product_prefix: str | None = None,
        block: str | list[str] | None = None,
        block_prefix: str | None = None,
        reserve_group: str | list[str] | None = None,
        reserve_group_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ReserveScenarioList:
        filter_ = _create_filter(
            self._view_id,
            min_volume,
            max_volume,
            auction,
            auction_prefix,
            product,
            product_prefix,
            block,
            block_prefix,
            reserve_group,
            reserve_group_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    min_volume: int | None = None,
    max_volume: int | None = None,
    auction: str | list[str] | None = None,
    auction_prefix: str | None = None,
    product: str | list[str] | None = None,
    product_prefix: str | None = None,
    block: str | list[str] | None = None,
    block_prefix: str | None = None,
    reserve_group: str | list[str] | None = None,
    reserve_group_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_volume or max_volume:
        filters.append(dm.filters.Range(view_id.as_property_ref("volume"), gte=min_volume, lte=max_volume))
    if auction and isinstance(auction, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("auction"), value=auction))
    if auction and isinstance(auction, list):
        filters.append(dm.filters.In(view_id.as_property_ref("auction"), values=auction))
    if auction_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("auction"), value=auction_prefix))
    if product and isinstance(product, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("product"), value=product))
    if product and isinstance(product, list):
        filters.append(dm.filters.In(view_id.as_property_ref("product"), values=product))
    if product_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("product"), value=product_prefix))
    if block and isinstance(block, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("block"), value=block))
    if block and isinstance(block, list):
        filters.append(dm.filters.In(view_id.as_property_ref("block"), values=block))
    if block_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("block"), value=block_prefix))
    if reserve_group and isinstance(reserve_group, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("reserveGroup"), value=reserve_group))
    if reserve_group and isinstance(reserve_group, list):
        filters.append(dm.filters.In(view_id.as_property_ref("reserveGroup"), values=reserve_group))
    if reserve_group_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("reserveGroup"), value=reserve_group_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
