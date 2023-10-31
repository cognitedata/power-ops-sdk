from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    ReserveBid,
    ReserveBidApply,
    ReserveBidApplyList,
    ReserveBidFields,
    ReserveBidList,
    ReserveBidTextFields,
)
from cognite.powerops.client._generated.data_classes._reserve_bid import _RESERVEBID_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, Aggregations, TypeAPI


class ReserveBidAPI(TypeAPI[ReserveBid, ReserveBidApply, ReserveBidList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ReserveBid,
            class_apply_type=ReserveBidApply,
            class_list=ReserveBidList,
        )
        self._view_id = view_id

    def apply(
        self, reserve_bid: ReserveBidApply | Sequence[ReserveBidApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(reserve_bid, ReserveBidApply):
            instances = reserve_bid.to_instances_apply()
        else:
            instances = ReserveBidApplyList(reserve_bid).to_instances_apply()
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
    def retrieve(self, external_id: str) -> ReserveBid:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ReserveBidList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> ReserveBid | ReserveBidList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: ReserveBidTextFields | Sequence[ReserveBidTextFields] | None = None,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        revision_number: str | list[str] | None = None,
        revision_number_prefix: str | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        process_type: str | list[str] | None = None,
        process_type_prefix: str | None = None,
        sender: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        receiver: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_created_date_time: datetime.datetime | None = None,
        max_created_date_time: datetime.datetime | None = None,
        domain: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        subject: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ReserveBidList:
        filter_ = _create_filter(
            self._view_id,
            m_rid,
            m_rid_prefix,
            revision_number,
            revision_number_prefix,
            type,
            type_prefix,
            process_type,
            process_type_prefix,
            sender,
            receiver,
            min_created_date_time,
            max_created_date_time,
            domain,
            subject,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _RESERVEBID_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ReserveBidFields | Sequence[ReserveBidFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: ReserveBidTextFields | Sequence[ReserveBidTextFields] | None = None,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        revision_number: str | list[str] | None = None,
        revision_number_prefix: str | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        process_type: str | list[str] | None = None,
        process_type_prefix: str | None = None,
        sender: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        receiver: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_created_date_time: datetime.datetime | None = None,
        max_created_date_time: datetime.datetime | None = None,
        domain: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        subject: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: ReserveBidFields | Sequence[ReserveBidFields] | None = None,
        group_by: ReserveBidFields | Sequence[ReserveBidFields] = None,
        query: str | None = None,
        search_properties: ReserveBidTextFields | Sequence[ReserveBidTextFields] | None = None,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        revision_number: str | list[str] | None = None,
        revision_number_prefix: str | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        process_type: str | list[str] | None = None,
        process_type_prefix: str | None = None,
        sender: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        receiver: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_created_date_time: datetime.datetime | None = None,
        max_created_date_time: datetime.datetime | None = None,
        domain: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        subject: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: ReserveBidFields | Sequence[ReserveBidFields] | None = None,
        group_by: ReserveBidFields | Sequence[ReserveBidFields] | None = None,
        query: str | None = None,
        search_property: ReserveBidTextFields | Sequence[ReserveBidTextFields] | None = None,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        revision_number: str | list[str] | None = None,
        revision_number_prefix: str | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        process_type: str | list[str] | None = None,
        process_type_prefix: str | None = None,
        sender: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        receiver: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_created_date_time: datetime.datetime | None = None,
        max_created_date_time: datetime.datetime | None = None,
        domain: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        subject: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            m_rid,
            m_rid_prefix,
            revision_number,
            revision_number_prefix,
            type,
            type_prefix,
            process_type,
            process_type_prefix,
            sender,
            receiver,
            min_created_date_time,
            max_created_date_time,
            domain,
            subject,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _RESERVEBID_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ReserveBidFields,
        interval: float,
        query: str | None = None,
        search_property: ReserveBidTextFields | Sequence[ReserveBidTextFields] | None = None,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        revision_number: str | list[str] | None = None,
        revision_number_prefix: str | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        process_type: str | list[str] | None = None,
        process_type_prefix: str | None = None,
        sender: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        receiver: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_created_date_time: datetime.datetime | None = None,
        max_created_date_time: datetime.datetime | None = None,
        domain: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        subject: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            m_rid,
            m_rid_prefix,
            revision_number,
            revision_number_prefix,
            type,
            type_prefix,
            process_type,
            process_type_prefix,
            sender,
            receiver,
            min_created_date_time,
            max_created_date_time,
            domain,
            subject,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _RESERVEBID_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        m_rid: str | list[str] | None = None,
        m_rid_prefix: str | None = None,
        revision_number: str | list[str] | None = None,
        revision_number_prefix: str | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        process_type: str | list[str] | None = None,
        process_type_prefix: str | None = None,
        sender: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        receiver: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_created_date_time: datetime.datetime | None = None,
        max_created_date_time: datetime.datetime | None = None,
        domain: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        subject: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ReserveBidList:
        filter_ = _create_filter(
            self._view_id,
            m_rid,
            m_rid_prefix,
            revision_number,
            revision_number_prefix,
            type,
            type_prefix,
            process_type,
            process_type_prefix,
            sender,
            receiver,
            min_created_date_time,
            max_created_date_time,
            domain,
            subject,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    m_rid: str | list[str] | None = None,
    m_rid_prefix: str | None = None,
    revision_number: str | list[str] | None = None,
    revision_number_prefix: str | None = None,
    type: str | list[str] | None = None,
    type_prefix: str | None = None,
    process_type: str | list[str] | None = None,
    process_type_prefix: str | None = None,
    sender: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    receiver: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    min_created_date_time: datetime.datetime | None = None,
    max_created_date_time: datetime.datetime | None = None,
    domain: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    subject: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if revision_number and isinstance(revision_number, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("revisionNumber"), value=revision_number))
    if revision_number and isinstance(revision_number, list):
        filters.append(dm.filters.In(view_id.as_property_ref("revisionNumber"), values=revision_number))
    if revision_number_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("revisionNumber"), value=revision_number_prefix))
    if type and isinstance(type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("type"), value=type))
    if type and isinstance(type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("type"), values=type))
    if type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("type"), value=type_prefix))
    if process_type and isinstance(process_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("processType"), value=process_type))
    if process_type and isinstance(process_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("processType"), values=process_type))
    if process_type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("processType"), value=process_type_prefix))
    if sender and isinstance(sender, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("sender"), value={"space": "power-ops", "externalId": sender})
        )
    if sender and isinstance(sender, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("sender"), value={"space": sender[0], "externalId": sender[1]})
        )
    if sender and isinstance(sender, list) and isinstance(sender[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("sender"),
                values=[{"space": "power-ops", "externalId": item} for item in sender],
            )
        )
    if sender and isinstance(sender, list) and isinstance(sender[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("sender"), values=[{"space": item[0], "externalId": item[1]} for item in sender]
            )
        )
    if receiver and isinstance(receiver, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("receiver"), value={"space": "power-ops", "externalId": receiver})
        )
    if receiver and isinstance(receiver, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("receiver"), value={"space": receiver[0], "externalId": receiver[1]}
            )
        )
    if receiver and isinstance(receiver, list) and isinstance(receiver[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("receiver"),
                values=[{"space": "power-ops", "externalId": item} for item in receiver],
            )
        )
    if receiver and isinstance(receiver, list) and isinstance(receiver[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("receiver"),
                values=[{"space": item[0], "externalId": item[1]} for item in receiver],
            )
        )
    if min_created_date_time or max_created_date_time:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("createdDateTime"),
                gte=min_created_date_time.isoformat() if min_created_date_time else None,
                lte=max_created_date_time.isoformat() if max_created_date_time else None,
            )
        )
    if domain and isinstance(domain, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("domain"), value={"space": "power-ops", "externalId": domain})
        )
    if domain and isinstance(domain, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("domain"), value={"space": domain[0], "externalId": domain[1]})
        )
    if domain and isinstance(domain, list) and isinstance(domain[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("domain"),
                values=[{"space": "power-ops", "externalId": item} for item in domain],
            )
        )
    if domain and isinstance(domain, list) and isinstance(domain[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("domain"), values=[{"space": item[0], "externalId": item[1]} for item in domain]
            )
        )
    if subject and isinstance(subject, str):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("subject"), value={"space": "power-ops", "externalId": subject})
        )
    if subject and isinstance(subject, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("subject"), value={"space": subject[0], "externalId": subject[1]})
        )
    if subject and isinstance(subject, list) and isinstance(subject[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("subject"),
                values=[{"space": "power-ops", "externalId": item} for item in subject],
            )
        )
    if subject and isinstance(subject, list) and isinstance(subject[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("subject"),
                values=[{"space": item[0], "externalId": item[1]} for item in subject],
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
