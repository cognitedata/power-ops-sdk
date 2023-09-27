from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import (
    BidTimeSeries,
    BidTimeSeriesApply,
    BidTimeSeriesApplyList,
    BidTimeSeriesList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class BidTimeSeriesAPI(TypeAPI[BidTimeSeries, BidTimeSeriesApply, BidTimeSeriesList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BidTimeSeries,
            class_apply_type=BidTimeSeriesApply,
            class_list=BidTimeSeriesList,
        )
        self.view_id = view_id

    def apply(
        self, bid_time_series: BidTimeSeriesApply | Sequence[BidTimeSeriesApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(bid_time_series, BidTimeSeriesApply):
            instances = bid_time_series.to_instances_apply()
        else:
            instances = BidTimeSeriesApplyList(bid_time_series).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(BidTimeSeriesApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(BidTimeSeriesApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> BidTimeSeries:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> BidTimeSeriesList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> BidTimeSeries | BidTimeSeriesList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(
        self,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        auction: str | list[str] | None = None,
        auction_prefix: str | None = None,
        quantity_measure_unit_name: str | list[str] | None = None,
        quantity_measure_unit_name_prefix: str | None = None,
        currency_unit_name: str | list[str] | None = None,
        currency_unit_name_prefix: str | None = None,
        price_measure_unit_name: str | list[str] | None = None,
        price_measure_unit_name_prefix: str | None = None,
        divisible: bool | None = None,
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
        standard_market_product_type: str | list[str] | None = None,
        standard_market_product_type_prefix: str | None = None,
        original_market_product_type: str | list[str] | None = None,
        original_market_product_type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidTimeSeriesList:
        filter_ = _create_filter(
            self.view_id,
            m_rid,
            m_rid_prefix,
            auction,
            auction_prefix,
            quantity_measure_unit_name,
            quantity_measure_unit_name_prefix,
            currency_unit_name,
            currency_unit_name_prefix,
            price_measure_unit_name,
            price_measure_unit_name_prefix,
            divisible,
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
            standard_market_product_type,
            standard_market_product_type_prefix,
            original_market_product_type,
            original_market_product_type_prefix,
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
    quantity_measure_unit_name: str | list[str] | None = None,
    quantity_measure_unit_name_prefix: str | None = None,
    currency_unit_name: str | list[str] | None = None,
    currency_unit_name_prefix: str | None = None,
    price_measure_unit_name: str | list[str] | None = None,
    price_measure_unit_name_prefix: str | None = None,
    divisible: bool | None = None,
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
    standard_market_product_type: str | list[str] | None = None,
    standard_market_product_type_prefix: str | None = None,
    original_market_product_type: str | list[str] | None = None,
    original_market_product_type_prefix: str | None = None,
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
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
