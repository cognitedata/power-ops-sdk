from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    BidTimeSeries,
    BidTimeSeriesApply,
    BidTimeSeriesApplyList,
    BidTimeSeriesFields,
    BidTimeSeriesList,
    BidTimeSeriesTextFields,
)
from cognite.powerops.client._generated.data_classes._bid_time_series import _BIDTIMESERIES_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, Aggregations, TypeAPI


class BidTimeSeriesAPI(TypeAPI[BidTimeSeries, BidTimeSeriesApply, BidTimeSeriesList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BidTimeSeries,
            class_apply_type=BidTimeSeriesApply,
            class_list=BidTimeSeriesList,
        )
        self._view_id = view_id

    def apply(
        self, bid_time_series: BidTimeSeriesApply | Sequence[BidTimeSeriesApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(bid_time_series, BidTimeSeriesApply):
            instances = bid_time_series.to_instances_apply()
        else:
            instances = BidTimeSeriesApplyList(bid_time_series).to_instances_apply()
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
    def retrieve(self, external_id: str) -> BidTimeSeries:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> BidTimeSeriesList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> BidTimeSeries | BidTimeSeriesList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: BidTimeSeriesTextFields | Sequence[BidTimeSeriesTextFields] | None = None,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        auction: str | list[str] | None = None,
        auction_prefix: str | None = None,
        acquiring_domain: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        connecting_domain: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        provider_market_participant: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        quantity_measure_unit_name: str | list[str] | None = None,
        quantity_measure_unit_name_prefix: str | None = None,
        currency_unit_name: str | list[str] | None = None,
        currency_unit_name_prefix: str | None = None,
        price_measure_unit_name: str | list[str] | None = None,
        price_measure_unit_name_prefix: str | None = None,
        divisible: bool | None = None,
        linked_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        multipart_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        exclusive_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        block_bid: bool | None = None,
        min_status: int | None = None,
        max_status: int | None = None,
        min_priority: int | None = None,
        max_priority: int | None = None,
        registered_resources_mrid: str | list[str] | None = None,
        registered_resources_mrid_prefix: str | None = None,
        flow_direction: str | list[str] | None = None,
        flow_direction_prefix: str | None = None,
        min_step_increment_quantity: float | None = None,
        max_step_increment_quantity: float | None = None,
        energy_price_measure_unit: str | list[str] | None = None,
        energy_price_measure_unit_prefix: str | None = None,
        market_agreement: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        activation_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        resting_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        minimum_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        maximum_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        standard_market_product_type: str | list[str] | None = None,
        standard_market_product_type_prefix: str | None = None,
        original_market_product_type: str | list[str] | None = None,
        original_market_product_type_prefix: str | None = None,
        validity_period: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        reason: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidTimeSeriesList:
        filter_ = _create_filter(
            self._view_id,
            m_rid,
            m_rid_prefix,
            auction,
            auction_prefix,
            acquiring_domain,
            connecting_domain,
            provider_market_participant,
            quantity_measure_unit_name,
            quantity_measure_unit_name_prefix,
            currency_unit_name,
            currency_unit_name_prefix,
            price_measure_unit_name,
            price_measure_unit_name_prefix,
            divisible,
            linked_bid,
            multipart_bid,
            exclusive_bid,
            block_bid,
            min_status,
            max_status,
            min_priority,
            max_priority,
            registered_resources_mrid,
            registered_resources_mrid_prefix,
            flow_direction,
            flow_direction_prefix,
            min_step_increment_quantity,
            max_step_increment_quantity,
            energy_price_measure_unit,
            energy_price_measure_unit_prefix,
            market_agreement,
            activation_constraint,
            resting_constraint,
            minimum_constraint,
            maximum_constraint,
            standard_market_product_type,
            standard_market_product_type_prefix,
            original_market_product_type,
            original_market_product_type_prefix,
            validity_period,
            reason,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _BIDTIMESERIES_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: BidTimeSeriesFields | Sequence[BidTimeSeriesFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: BidTimeSeriesTextFields | Sequence[BidTimeSeriesTextFields] | None = None,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        auction: str | list[str] | None = None,
        auction_prefix: str | None = None,
        acquiring_domain: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        connecting_domain: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        provider_market_participant: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        quantity_measure_unit_name: str | list[str] | None = None,
        quantity_measure_unit_name_prefix: str | None = None,
        currency_unit_name: str | list[str] | None = None,
        currency_unit_name_prefix: str | None = None,
        price_measure_unit_name: str | list[str] | None = None,
        price_measure_unit_name_prefix: str | None = None,
        divisible: bool | None = None,
        linked_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        multipart_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        exclusive_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        block_bid: bool | None = None,
        min_status: int | None = None,
        max_status: int | None = None,
        min_priority: int | None = None,
        max_priority: int | None = None,
        registered_resources_mrid: str | list[str] | None = None,
        registered_resources_mrid_prefix: str | None = None,
        flow_direction: str | list[str] | None = None,
        flow_direction_prefix: str | None = None,
        min_step_increment_quantity: float | None = None,
        max_step_increment_quantity: float | None = None,
        energy_price_measure_unit: str | list[str] | None = None,
        energy_price_measure_unit_prefix: str | None = None,
        market_agreement: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        activation_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        resting_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        minimum_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        maximum_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        standard_market_product_type: str | list[str] | None = None,
        standard_market_product_type_prefix: str | None = None,
        original_market_product_type: str | list[str] | None = None,
        original_market_product_type_prefix: str | None = None,
        validity_period: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        reason: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: BidTimeSeriesFields | Sequence[BidTimeSeriesFields] | None = None,
        group_by: BidTimeSeriesFields | Sequence[BidTimeSeriesFields] = None,
        query: str | None = None,
        search_properties: BidTimeSeriesTextFields | Sequence[BidTimeSeriesTextFields] | None = None,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        auction: str | list[str] | None = None,
        auction_prefix: str | None = None,
        acquiring_domain: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        connecting_domain: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        provider_market_participant: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        quantity_measure_unit_name: str | list[str] | None = None,
        quantity_measure_unit_name_prefix: str | None = None,
        currency_unit_name: str | list[str] | None = None,
        currency_unit_name_prefix: str | None = None,
        price_measure_unit_name: str | list[str] | None = None,
        price_measure_unit_name_prefix: str | None = None,
        divisible: bool | None = None,
        linked_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        multipart_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        exclusive_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        block_bid: bool | None = None,
        min_status: int | None = None,
        max_status: int | None = None,
        min_priority: int | None = None,
        max_priority: int | None = None,
        registered_resources_mrid: str | list[str] | None = None,
        registered_resources_mrid_prefix: str | None = None,
        flow_direction: str | list[str] | None = None,
        flow_direction_prefix: str | None = None,
        min_step_increment_quantity: float | None = None,
        max_step_increment_quantity: float | None = None,
        energy_price_measure_unit: str | list[str] | None = None,
        energy_price_measure_unit_prefix: str | None = None,
        market_agreement: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        activation_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        resting_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        minimum_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        maximum_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        standard_market_product_type: str | list[str] | None = None,
        standard_market_product_type_prefix: str | None = None,
        original_market_product_type: str | list[str] | None = None,
        original_market_product_type_prefix: str | None = None,
        validity_period: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        reason: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: BidTimeSeriesFields | Sequence[BidTimeSeriesFields] | None = None,
        group_by: BidTimeSeriesFields | Sequence[BidTimeSeriesFields] | None = None,
        query: str | None = None,
        search_property: BidTimeSeriesTextFields | Sequence[BidTimeSeriesTextFields] | None = None,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        auction: str | list[str] | None = None,
        auction_prefix: str | None = None,
        acquiring_domain: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        connecting_domain: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        provider_market_participant: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        quantity_measure_unit_name: str | list[str] | None = None,
        quantity_measure_unit_name_prefix: str | None = None,
        currency_unit_name: str | list[str] | None = None,
        currency_unit_name_prefix: str | None = None,
        price_measure_unit_name: str | list[str] | None = None,
        price_measure_unit_name_prefix: str | None = None,
        divisible: bool | None = None,
        linked_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        multipart_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        exclusive_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        block_bid: bool | None = None,
        min_status: int | None = None,
        max_status: int | None = None,
        min_priority: int | None = None,
        max_priority: int | None = None,
        registered_resources_mrid: str | list[str] | None = None,
        registered_resources_mrid_prefix: str | None = None,
        flow_direction: str | list[str] | None = None,
        flow_direction_prefix: str | None = None,
        min_step_increment_quantity: float | None = None,
        max_step_increment_quantity: float | None = None,
        energy_price_measure_unit: str | list[str] | None = None,
        energy_price_measure_unit_prefix: str | None = None,
        market_agreement: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        activation_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        resting_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        minimum_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        maximum_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        standard_market_product_type: str | list[str] | None = None,
        standard_market_product_type_prefix: str | None = None,
        original_market_product_type: str | list[str] | None = None,
        original_market_product_type_prefix: str | None = None,
        validity_period: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        reason: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            m_rid,
            m_rid_prefix,
            auction,
            auction_prefix,
            acquiring_domain,
            connecting_domain,
            provider_market_participant,
            quantity_measure_unit_name,
            quantity_measure_unit_name_prefix,
            currency_unit_name,
            currency_unit_name_prefix,
            price_measure_unit_name,
            price_measure_unit_name_prefix,
            divisible,
            linked_bid,
            multipart_bid,
            exclusive_bid,
            block_bid,
            min_status,
            max_status,
            min_priority,
            max_priority,
            registered_resources_mrid,
            registered_resources_mrid_prefix,
            flow_direction,
            flow_direction_prefix,
            min_step_increment_quantity,
            max_step_increment_quantity,
            energy_price_measure_unit,
            energy_price_measure_unit_prefix,
            market_agreement,
            activation_constraint,
            resting_constraint,
            minimum_constraint,
            maximum_constraint,
            standard_market_product_type,
            standard_market_product_type_prefix,
            original_market_product_type,
            original_market_product_type_prefix,
            validity_period,
            reason,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _BIDTIMESERIES_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: BidTimeSeriesFields,
        interval: float,
        query: str | None = None,
        search_property: BidTimeSeriesTextFields | Sequence[BidTimeSeriesTextFields] | None = None,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        auction: str | list[str] | None = None,
        auction_prefix: str | None = None,
        acquiring_domain: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        connecting_domain: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        provider_market_participant: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        quantity_measure_unit_name: str | list[str] | None = None,
        quantity_measure_unit_name_prefix: str | None = None,
        currency_unit_name: str | list[str] | None = None,
        currency_unit_name_prefix: str | None = None,
        price_measure_unit_name: str | list[str] | None = None,
        price_measure_unit_name_prefix: str | None = None,
        divisible: bool | None = None,
        linked_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        multipart_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        exclusive_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        block_bid: bool | None = None,
        min_status: int | None = None,
        max_status: int | None = None,
        min_priority: int | None = None,
        max_priority: int | None = None,
        registered_resources_mrid: str | list[str] | None = None,
        registered_resources_mrid_prefix: str | None = None,
        flow_direction: str | list[str] | None = None,
        flow_direction_prefix: str | None = None,
        min_step_increment_quantity: float | None = None,
        max_step_increment_quantity: float | None = None,
        energy_price_measure_unit: str | list[str] | None = None,
        energy_price_measure_unit_prefix: str | None = None,
        market_agreement: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        activation_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        resting_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        minimum_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        maximum_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        standard_market_product_type: str | list[str] | None = None,
        standard_market_product_type_prefix: str | None = None,
        original_market_product_type: str | list[str] | None = None,
        original_market_product_type_prefix: str | None = None,
        validity_period: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        reason: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            m_rid,
            m_rid_prefix,
            auction,
            auction_prefix,
            acquiring_domain,
            connecting_domain,
            provider_market_participant,
            quantity_measure_unit_name,
            quantity_measure_unit_name_prefix,
            currency_unit_name,
            currency_unit_name_prefix,
            price_measure_unit_name,
            price_measure_unit_name_prefix,
            divisible,
            linked_bid,
            multipart_bid,
            exclusive_bid,
            block_bid,
            min_status,
            max_status,
            min_priority,
            max_priority,
            registered_resources_mrid,
            registered_resources_mrid_prefix,
            flow_direction,
            flow_direction_prefix,
            min_step_increment_quantity,
            max_step_increment_quantity,
            energy_price_measure_unit,
            energy_price_measure_unit_prefix,
            market_agreement,
            activation_constraint,
            resting_constraint,
            minimum_constraint,
            maximum_constraint,
            standard_market_product_type,
            standard_market_product_type_prefix,
            original_market_product_type,
            original_market_product_type_prefix,
            validity_period,
            reason,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _BIDTIMESERIES_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        auction: str | list[str] | None = None,
        auction_prefix: str | None = None,
        acquiring_domain: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        connecting_domain: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        provider_market_participant: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        quantity_measure_unit_name: str | list[str] | None = None,
        quantity_measure_unit_name_prefix: str | None = None,
        currency_unit_name: str | list[str] | None = None,
        currency_unit_name_prefix: str | None = None,
        price_measure_unit_name: str | list[str] | None = None,
        price_measure_unit_name_prefix: str | None = None,
        divisible: bool | None = None,
        linked_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        multipart_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        exclusive_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        block_bid: bool | None = None,
        min_status: int | None = None,
        max_status: int | None = None,
        min_priority: int | None = None,
        max_priority: int | None = None,
        registered_resources_mrid: str | list[str] | None = None,
        registered_resources_mrid_prefix: str | None = None,
        flow_direction: str | list[str] | None = None,
        flow_direction_prefix: str | None = None,
        min_step_increment_quantity: float | None = None,
        max_step_increment_quantity: float | None = None,
        energy_price_measure_unit: str | list[str] | None = None,
        energy_price_measure_unit_prefix: str | None = None,
        market_agreement: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        activation_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        resting_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        minimum_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        maximum_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        standard_market_product_type: str | list[str] | None = None,
        standard_market_product_type_prefix: str | None = None,
        original_market_product_type: str | list[str] | None = None,
        original_market_product_type_prefix: str | None = None,
        validity_period: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        reason: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidTimeSeriesList:
        filter_ = _create_filter(
            self._view_id,
            m_rid,
            m_rid_prefix,
            auction,
            auction_prefix,
            acquiring_domain,
            connecting_domain,
            provider_market_participant,
            quantity_measure_unit_name,
            quantity_measure_unit_name_prefix,
            currency_unit_name,
            currency_unit_name_prefix,
            price_measure_unit_name,
            price_measure_unit_name_prefix,
            divisible,
            linked_bid,
            multipart_bid,
            exclusive_bid,
            block_bid,
            min_status,
            max_status,
            min_priority,
            max_priority,
            registered_resources_mrid,
            registered_resources_mrid_prefix,
            flow_direction,
            flow_direction_prefix,
            min_step_increment_quantity,
            max_step_increment_quantity,
            energy_price_measure_unit,
            energy_price_measure_unit_prefix,
            market_agreement,
            activation_constraint,
            resting_constraint,
            minimum_constraint,
            maximum_constraint,
            standard_market_product_type,
            standard_market_product_type_prefix,
            original_market_product_type,
            original_market_product_type_prefix,
            validity_period,
            reason,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    m_rid: str | list[str] | None = None,
    m_rid_prefix: str | None = None,
    auction: str | list[str] | None = None,
    auction_prefix: str | None = None,
    acquiring_domain: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    connecting_domain: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    provider_market_participant: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    quantity_measure_unit_name: str | list[str] | None = None,
    quantity_measure_unit_name_prefix: str | None = None,
    currency_unit_name: str | list[str] | None = None,
    currency_unit_name_prefix: str | None = None,
    price_measure_unit_name: str | list[str] | None = None,
    price_measure_unit_name_prefix: str | None = None,
    divisible: bool | None = None,
    linked_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    multipart_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    exclusive_bid: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    block_bid: bool | None = None,
    min_status: int | None = None,
    max_status: int | None = None,
    min_priority: int | None = None,
    max_priority: int | None = None,
    registered_resources_mrid: str | list[str] | None = None,
    registered_resources_mrid_prefix: str | None = None,
    flow_direction: str | list[str] | None = None,
    flow_direction_prefix: str | None = None,
    min_step_increment_quantity: float | None = None,
    max_step_increment_quantity: float | None = None,
    energy_price_measure_unit: str | list[str] | None = None,
    energy_price_measure_unit_prefix: str | None = None,
    market_agreement: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    activation_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    resting_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    minimum_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    maximum_constraint: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    standard_market_product_type: str | list[str] | None = None,
    standard_market_product_type_prefix: str | None = None,
    original_market_product_type: str | list[str] | None = None,
    original_market_product_type_prefix: str | None = None,
    validity_period: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    reason: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if m_rid and isinstance(m_rid, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("mRID"), value=m_rid))
    if m_rid and isinstance(m_rid, list):
        filters.append(dm.filters.In(view_id.as_property_ref("mRID"), values=m_rid))
    if m_rid_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("mRID"), value=m_rid_prefix))
    if auction and isinstance(auction, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("auction"), value=auction))
    if auction and isinstance(auction, list):
        filters.append(dm.filters.In(view_id.as_property_ref("auction"), values=auction))
    if auction_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("auction"), value=auction_prefix))
    if acquiring_domain and isinstance(acquiring_domain, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("acquiringDomain"), value={"space": "power-ops", "externalId": acquiring_domain}
            )
        )
    if acquiring_domain and isinstance(acquiring_domain, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("acquiringDomain"),
                value={"space": acquiring_domain[0], "externalId": acquiring_domain[1]},
            )
        )
    if acquiring_domain and isinstance(acquiring_domain, list) and isinstance(acquiring_domain[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("acquiringDomain"),
                values=[{"space": "power-ops", "externalId": item} for item in acquiring_domain],
            )
        )
    if acquiring_domain and isinstance(acquiring_domain, list) and isinstance(acquiring_domain[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("acquiringDomain"),
                values=[{"space": item[0], "externalId": item[1]} for item in acquiring_domain],
            )
        )
    if connecting_domain and isinstance(connecting_domain, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("connectingDomain"),
                value={"space": "power-ops", "externalId": connecting_domain},
            )
        )
    if connecting_domain and isinstance(connecting_domain, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("connectingDomain"),
                value={"space": connecting_domain[0], "externalId": connecting_domain[1]},
            )
        )
    if connecting_domain and isinstance(connecting_domain, list) and isinstance(connecting_domain[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("connectingDomain"),
                values=[{"space": "power-ops", "externalId": item} for item in connecting_domain],
            )
        )
    if connecting_domain and isinstance(connecting_domain, list) and isinstance(connecting_domain[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("connectingDomain"),
                values=[{"space": item[0], "externalId": item[1]} for item in connecting_domain],
            )
        )
    if provider_market_participant and isinstance(provider_market_participant, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("providerMarketParticipant"),
                value={"space": "power-ops", "externalId": provider_market_participant},
            )
        )
    if provider_market_participant and isinstance(provider_market_participant, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("providerMarketParticipant"),
                value={"space": provider_market_participant[0], "externalId": provider_market_participant[1]},
            )
        )
    if (
        provider_market_participant
        and isinstance(provider_market_participant, list)
        and isinstance(provider_market_participant[0], str)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("providerMarketParticipant"),
                values=[{"space": "power-ops", "externalId": item} for item in provider_market_participant],
            )
        )
    if (
        provider_market_participant
        and isinstance(provider_market_participant, list)
        and isinstance(provider_market_participant[0], tuple)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("providerMarketParticipant"),
                values=[{"space": item[0], "externalId": item[1]} for item in provider_market_participant],
            )
        )
    if quantity_measure_unit_name and isinstance(quantity_measure_unit_name, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("quantityMeasureUnitName"), value=quantity_measure_unit_name)
        )
    if quantity_measure_unit_name and isinstance(quantity_measure_unit_name, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("quantityMeasureUnitName"), values=quantity_measure_unit_name)
        )
    if quantity_measure_unit_name_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("quantityMeasureUnitName"), value=quantity_measure_unit_name_prefix
            )
        )
    if currency_unit_name and isinstance(currency_unit_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("currencyUnitName"), value=currency_unit_name))
    if currency_unit_name and isinstance(currency_unit_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("currencyUnitName"), values=currency_unit_name))
    if currency_unit_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("currencyUnitName"), value=currency_unit_name_prefix))
    if price_measure_unit_name and isinstance(price_measure_unit_name, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("priceMeasureUnitName"), value=price_measure_unit_name)
        )
    if price_measure_unit_name and isinstance(price_measure_unit_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("priceMeasureUnitName"), values=price_measure_unit_name))
    if price_measure_unit_name_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("priceMeasureUnitName"), value=price_measure_unit_name_prefix)
        )
    if divisible and isinstance(divisible, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("divisible"), value=divisible))
    if linked_bid and isinstance(linked_bid, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("linkedBid"), value={"space": "power-ops", "externalId": linked_bid}
            )
        )
    if linked_bid and isinstance(linked_bid, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("linkedBid"), value={"space": linked_bid[0], "externalId": linked_bid[1]}
            )
        )
    if linked_bid and isinstance(linked_bid, list) and isinstance(linked_bid[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("linkedBid"),
                values=[{"space": "power-ops", "externalId": item} for item in linked_bid],
            )
        )
    if linked_bid and isinstance(linked_bid, list) and isinstance(linked_bid[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("linkedBid"),
                values=[{"space": item[0], "externalId": item[1]} for item in linked_bid],
            )
        )
    if multipart_bid and isinstance(multipart_bid, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("multipartBid"), value={"space": "power-ops", "externalId": multipart_bid}
            )
        )
    if multipart_bid and isinstance(multipart_bid, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("multipartBid"),
                value={"space": multipart_bid[0], "externalId": multipart_bid[1]},
            )
        )
    if multipart_bid and isinstance(multipart_bid, list) and isinstance(multipart_bid[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("multipartBid"),
                values=[{"space": "power-ops", "externalId": item} for item in multipart_bid],
            )
        )
    if multipart_bid and isinstance(multipart_bid, list) and isinstance(multipart_bid[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("multipartBid"),
                values=[{"space": item[0], "externalId": item[1]} for item in multipart_bid],
            )
        )
    if exclusive_bid and isinstance(exclusive_bid, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("exclusiveBid"), value={"space": "power-ops", "externalId": exclusive_bid}
            )
        )
    if exclusive_bid and isinstance(exclusive_bid, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("exclusiveBid"),
                value={"space": exclusive_bid[0], "externalId": exclusive_bid[1]},
            )
        )
    if exclusive_bid and isinstance(exclusive_bid, list) and isinstance(exclusive_bid[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("exclusiveBid"),
                values=[{"space": "power-ops", "externalId": item} for item in exclusive_bid],
            )
        )
    if exclusive_bid and isinstance(exclusive_bid, list) and isinstance(exclusive_bid[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("exclusiveBid"),
                values=[{"space": item[0], "externalId": item[1]} for item in exclusive_bid],
            )
        )
    if block_bid and isinstance(block_bid, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("blockBid"), value=block_bid))
    if min_status or max_status:
        filters.append(dm.filters.Range(view_id.as_property_ref("status"), gte=min_status, lte=max_status))
    if min_priority or max_priority:
        filters.append(dm.filters.Range(view_id.as_property_ref("priority"), gte=min_priority, lte=max_priority))
    if registered_resources_mrid and isinstance(registered_resources_mrid, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("registeredResourcesMRID"), value=registered_resources_mrid)
        )
    if registered_resources_mrid and isinstance(registered_resources_mrid, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("registeredResourcesMRID"), values=registered_resources_mrid)
        )
    if registered_resources_mrid_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("registeredResourcesMRID"), value=registered_resources_mrid_prefix
            )
        )
    if flow_direction and isinstance(flow_direction, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("flowDirection"), value=flow_direction))
    if flow_direction and isinstance(flow_direction, list):
        filters.append(dm.filters.In(view_id.as_property_ref("flowDirection"), values=flow_direction))
    if flow_direction_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("flowDirection"), value=flow_direction_prefix))
    if min_step_increment_quantity or max_step_increment_quantity:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("stepIncrementQuantity"),
                gte=min_step_increment_quantity,
                lte=max_step_increment_quantity,
            )
        )
    if energy_price_measure_unit and isinstance(energy_price_measure_unit, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("energyPriceMeasureUnit"), value=energy_price_measure_unit)
        )
    if energy_price_measure_unit and isinstance(energy_price_measure_unit, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("energyPriceMeasureUnit"), values=energy_price_measure_unit)
        )
    if energy_price_measure_unit_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("energyPriceMeasureUnit"), value=energy_price_measure_unit_prefix)
        )
    if market_agreement and isinstance(market_agreement, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("marketAgreement"), value={"space": "power-ops", "externalId": market_agreement}
            )
        )
    if market_agreement and isinstance(market_agreement, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("marketAgreement"),
                value={"space": market_agreement[0], "externalId": market_agreement[1]},
            )
        )
    if market_agreement and isinstance(market_agreement, list) and isinstance(market_agreement[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("marketAgreement"),
                values=[{"space": "power-ops", "externalId": item} for item in market_agreement],
            )
        )
    if market_agreement and isinstance(market_agreement, list) and isinstance(market_agreement[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("marketAgreement"),
                values=[{"space": item[0], "externalId": item[1]} for item in market_agreement],
            )
        )
    if activation_constraint and isinstance(activation_constraint, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("activationConstraint"),
                value={"space": "power-ops", "externalId": activation_constraint},
            )
        )
    if activation_constraint and isinstance(activation_constraint, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("activationConstraint"),
                value={"space": activation_constraint[0], "externalId": activation_constraint[1]},
            )
        )
    if activation_constraint and isinstance(activation_constraint, list) and isinstance(activation_constraint[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("activationConstraint"),
                values=[{"space": "power-ops", "externalId": item} for item in activation_constraint],
            )
        )
    if (
        activation_constraint
        and isinstance(activation_constraint, list)
        and isinstance(activation_constraint[0], tuple)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("activationConstraint"),
                values=[{"space": item[0], "externalId": item[1]} for item in activation_constraint],
            )
        )
    if resting_constraint and isinstance(resting_constraint, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("restingConstraint"),
                value={"space": "power-ops", "externalId": resting_constraint},
            )
        )
    if resting_constraint and isinstance(resting_constraint, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("restingConstraint"),
                value={"space": resting_constraint[0], "externalId": resting_constraint[1]},
            )
        )
    if resting_constraint and isinstance(resting_constraint, list) and isinstance(resting_constraint[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("restingConstraint"),
                values=[{"space": "power-ops", "externalId": item} for item in resting_constraint],
            )
        )
    if resting_constraint and isinstance(resting_constraint, list) and isinstance(resting_constraint[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("restingConstraint"),
                values=[{"space": item[0], "externalId": item[1]} for item in resting_constraint],
            )
        )
    if minimum_constraint and isinstance(minimum_constraint, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("minimumConstraint"),
                value={"space": "power-ops", "externalId": minimum_constraint},
            )
        )
    if minimum_constraint and isinstance(minimum_constraint, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("minimumConstraint"),
                value={"space": minimum_constraint[0], "externalId": minimum_constraint[1]},
            )
        )
    if minimum_constraint and isinstance(minimum_constraint, list) and isinstance(minimum_constraint[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("minimumConstraint"),
                values=[{"space": "power-ops", "externalId": item} for item in minimum_constraint],
            )
        )
    if minimum_constraint and isinstance(minimum_constraint, list) and isinstance(minimum_constraint[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("minimumConstraint"),
                values=[{"space": item[0], "externalId": item[1]} for item in minimum_constraint],
            )
        )
    if maximum_constraint and isinstance(maximum_constraint, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("maximumConstraint"),
                value={"space": "power-ops", "externalId": maximum_constraint},
            )
        )
    if maximum_constraint and isinstance(maximum_constraint, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("maximumConstraint"),
                value={"space": maximum_constraint[0], "externalId": maximum_constraint[1]},
            )
        )
    if maximum_constraint and isinstance(maximum_constraint, list) and isinstance(maximum_constraint[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("maximumConstraint"),
                values=[{"space": "power-ops", "externalId": item} for item in maximum_constraint],
            )
        )
    if maximum_constraint and isinstance(maximum_constraint, list) and isinstance(maximum_constraint[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("maximumConstraint"),
                values=[{"space": item[0], "externalId": item[1]} for item in maximum_constraint],
            )
        )
    if standard_market_product_type and isinstance(standard_market_product_type, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("standardMarketProductType"), value=standard_market_product_type)
        )
    if standard_market_product_type and isinstance(standard_market_product_type, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("standardMarketProductType"), values=standard_market_product_type)
        )
    if standard_market_product_type_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("standardMarketProductType"), value=standard_market_product_type_prefix
            )
        )
    if original_market_product_type and isinstance(original_market_product_type, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("originalMarketProductType"), value=original_market_product_type)
        )
    if original_market_product_type and isinstance(original_market_product_type, list):
        filters.append(
            dm.filters.In(view_id.as_property_ref("originalMarketProductType"), values=original_market_product_type)
        )
    if original_market_product_type_prefix:
        filters.append(
            dm.filters.Prefix(
                view_id.as_property_ref("originalMarketProductType"), value=original_market_product_type_prefix
            )
        )
    if validity_period and isinstance(validity_period, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("validityPeriod"), value={"space": "power-ops", "externalId": validity_period}
            )
        )
    if validity_period and isinstance(validity_period, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("validityPeriod"),
                value={"space": validity_period[0], "externalId": validity_period[1]},
            )
        )
    if validity_period and isinstance(validity_period, list) and isinstance(validity_period[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("validityPeriod"),
                values=[{"space": "power-ops", "externalId": item} for item in validity_period],
            )
        )
    if validity_period and isinstance(validity_period, list) and isinstance(validity_period[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("validityPeriod"),
                values=[{"space": item[0], "externalId": item[1]} for item in validity_period],
            )
        )
    if reason and isinstance(reason, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("reason"), value={"space": "power-ops", "externalId": reason})
        )
    if reason and isinstance(reason, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("reason"), value={"space": reason[0], "externalId": reason[1]})
        )
    if reason and isinstance(reason, list) and isinstance(reason[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("reason"),
                values=[{"space": "power-ops", "externalId": item} for item in reason],
            )
        )
    if reason and isinstance(reason, list) and isinstance(reason[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("reason"), values=[{"space": item[0], "externalId": item[1]} for item in reason]
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
