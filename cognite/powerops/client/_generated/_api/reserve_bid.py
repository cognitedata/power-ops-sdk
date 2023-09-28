from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import (
    ReserveBid,
    ReserveBidApply,
    ReserveBidApplyList,
    ReserveBidList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class ReserveBidAPI(TypeAPI[ReserveBid, ReserveBidApply, ReserveBidList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ReserveBid,
            class_apply_type=ReserveBidApply,
            class_list=ReserveBidList,
        )
        self.view_id = view_id

    def apply(
        self, reserve_bid: ReserveBidApply | Sequence[ReserveBidApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(reserve_bid, ReserveBidApply):
            instances = reserve_bid.to_instances_apply()
        else:
            instances = ReserveBidApplyList(reserve_bid).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(ReserveBidApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(ReserveBidApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> ReserveBid:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ReserveBidList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> ReserveBid | ReserveBidList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

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
        min_created_date_time: datetime.datetime | None = None,
        max_created_date_time: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ReserveBidList:
        filter_ = _create_filter(
            self.view_id,
            m_rid,
            m_rid_prefix,
            revision_number,
            revision_number_prefix,
            type,
            type_prefix,
            process_type,
            process_type_prefix,
            min_created_date_time,
            max_created_date_time,
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
    min_created_date_time: datetime.datetime | None = None,
    max_created_date_time: datetime.datetime | None = None,
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
    if min_created_date_time or max_created_date_time:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("createdDateTime"),
                gte=min_created_date_time.isoformat() if min_created_date_time else None,
                lte=max_created_date_time.isoformat() if max_created_date_time else None,
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
