from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    MarketAgreement,
    MarketAgreementApply,
    MarketAgreementApplyList,
    MarketAgreementFields,
    MarketAgreementList,
    MarketAgreementTextFields,
)
from cognite.powerops.client._generated.data_classes._market_agreement import _MARKETAGREEMENT_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, Aggregations, TypeAPI


class MarketAgreementAPI(TypeAPI[MarketAgreement, MarketAgreementApply, MarketAgreementList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=MarketAgreement,
            class_apply_type=MarketAgreementApply,
            class_list=MarketAgreementList,
        )
        self._view_id = view_id

    def apply(
        self, market_agreement: MarketAgreementApply | Sequence[MarketAgreementApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(market_agreement, MarketAgreementApply):
            instances = market_agreement.to_instances_apply()
        else:
            instances = MarketAgreementApplyList(market_agreement).to_instances_apply()
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
    def retrieve(self, external_id: str) -> MarketAgreement:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> MarketAgreementList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> MarketAgreement | MarketAgreementList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: MarketAgreementTextFields | Sequence[MarketAgreementTextFields] | None = None,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        min_created_timestamp: datetime.datetime | None = None,
        max_created_timestamp: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MarketAgreementList:
        filter_ = _create_filter(
            self._view_id,
            m_rid,
            m_rid_prefix,
            type,
            type_prefix,
            min_created_timestamp,
            max_created_timestamp,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _MARKETAGREEMENT_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: MarketAgreementFields | Sequence[MarketAgreementFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: MarketAgreementTextFields | Sequence[MarketAgreementTextFields] | None = None,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        min_created_timestamp: datetime.datetime | None = None,
        max_created_timestamp: datetime.datetime | None = None,
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
        property: MarketAgreementFields | Sequence[MarketAgreementFields] | None = None,
        group_by: MarketAgreementFields | Sequence[MarketAgreementFields] = None,
        query: str | None = None,
        search_properties: MarketAgreementTextFields | Sequence[MarketAgreementTextFields] | None = None,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        min_created_timestamp: datetime.datetime | None = None,
        max_created_timestamp: datetime.datetime | None = None,
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
        property: MarketAgreementFields | Sequence[MarketAgreementFields] | None = None,
        group_by: MarketAgreementFields | Sequence[MarketAgreementFields] | None = None,
        query: str | None = None,
        search_property: MarketAgreementTextFields | Sequence[MarketAgreementTextFields] | None = None,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        min_created_timestamp: datetime.datetime | None = None,
        max_created_timestamp: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            m_rid,
            m_rid_prefix,
            type,
            type_prefix,
            min_created_timestamp,
            max_created_timestamp,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _MARKETAGREEMENT_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: MarketAgreementFields,
        interval: float,
        query: str | None = None,
        search_property: MarketAgreementTextFields | Sequence[MarketAgreementTextFields] | None = None,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        min_created_timestamp: datetime.datetime | None = None,
        max_created_timestamp: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            m_rid,
            m_rid_prefix,
            type,
            type_prefix,
            min_created_timestamp,
            max_created_timestamp,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _MARKETAGREEMENT_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        min_created_timestamp: datetime.datetime | None = None,
        max_created_timestamp: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MarketAgreementList:
        filter_ = _create_filter(
            self._view_id,
            m_rid,
            m_rid_prefix,
            type,
            type_prefix,
            min_created_timestamp,
            max_created_timestamp,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    m_rid: str | list[str] | None = None,
    m_rid_prefix: str | None = None,
    type: str | list[str] | None = None,
    type_prefix: str | None = None,
    min_created_timestamp: datetime.datetime | None = None,
    max_created_timestamp: datetime.datetime | None = None,
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
    if type and isinstance(type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("type"), value=type))
    if type and isinstance(type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("type"), values=type))
    if type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("type"), value=type_prefix))
    if min_created_timestamp or max_created_timestamp:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("createdTimestamp"),
                gte=min_created_timestamp.isoformat() if min_created_timestamp else None,
                lte=max_created_timestamp.isoformat() if max_created_timestamp else None,
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
