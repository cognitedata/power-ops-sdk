from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    MBADomain,
    MBADomainApply,
    MBADomainApplyList,
    MBADomainFields,
    MBADomainList,
    MBADomainTextFields,
)
from cognite.powerops.client._generated.data_classes._mba_domain import _MBADOMAIN_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, Aggregations, TypeAPI


class MBADomainAPI(TypeAPI[MBADomain, MBADomainApply, MBADomainList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=MBADomain,
            class_apply_type=MBADomainApply,
            class_list=MBADomainList,
        )
        self._view_id = view_id

    def apply(
        self, mba_domain: MBADomainApply | Sequence[MBADomainApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(mba_domain, MBADomainApply):
            instances = mba_domain.to_instances_apply()
        else:
            instances = MBADomainApplyList(mba_domain).to_instances_apply()
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
    def retrieve(self, external_id: str) -> MBADomain:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> MBADomainList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> MBADomain | MBADomainList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: MBADomainTextFields | Sequence[MBADomainTextFields] | None = None,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MBADomainList:
        filter_ = _create_filter(
            self._view_id,
            m_rid,
            m_rid_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _MBADOMAIN_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: MBADomainFields | Sequence[MBADomainFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: MBADomainTextFields | Sequence[MBADomainTextFields] | None = None,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
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
        property: MBADomainFields | Sequence[MBADomainFields] | None = None,
        group_by: MBADomainFields | Sequence[MBADomainFields] = None,
        query: str | None = None,
        search_properties: MBADomainTextFields | Sequence[MBADomainTextFields] | None = None,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
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
        property: MBADomainFields | Sequence[MBADomainFields] | None = None,
        group_by: MBADomainFields | Sequence[MBADomainFields] | None = None,
        query: str | None = None,
        search_property: MBADomainTextFields | Sequence[MBADomainTextFields] | None = None,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            m_rid,
            m_rid_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _MBADOMAIN_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: MBADomainFields,
        interval: float,
        query: str | None = None,
        search_property: MBADomainTextFields | Sequence[MBADomainTextFields] | None = None,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            m_rid,
            m_rid_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _MBADOMAIN_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MBADomainList:
        filter_ = _create_filter(
            self._view_id,
            m_rid,
            m_rid_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    m_rid: str | list[str] | None = None,
    m_rid_prefix: str | None = None,
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
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
