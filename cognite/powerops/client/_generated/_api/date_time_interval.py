from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import (
    DateTimeInterval,
    DateTimeIntervalApply,
    DateTimeIntervalApplyList,
    DateTimeIntervalList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class DateTimeIntervalAPI(TypeAPI[DateTimeInterval, DateTimeIntervalApply, DateTimeIntervalList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=DateTimeInterval,
            class_apply_type=DateTimeIntervalApply,
            class_list=DateTimeIntervalList,
        )
        self.view_id = view_id

    def apply(
        self, date_time_interval: DateTimeIntervalApply | Sequence[DateTimeIntervalApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(date_time_interval, DateTimeIntervalApply):
            instances = date_time_interval.to_instances_apply()
        else:
            instances = DateTimeIntervalApplyList(date_time_interval).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(DateTimeIntervalApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(DateTimeIntervalApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> DateTimeInterval:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> DateTimeIntervalList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> DateTimeInterval | DateTimeIntervalList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(
        self,
        min_start: datetime.datetime | None = None,
        max_start: datetime.datetime | None = None,
        min_end: datetime.datetime | None = None,
        max_end: datetime.datetime | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> DateTimeIntervalList:
        filter_ = _create_filter(
            self.view_id,
            min_start,
            max_start,
            min_end,
            max_end,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    min_start: datetime.datetime | None = None,
    max_start: datetime.datetime | None = None,
    min_end: datetime.datetime | None = None,
    max_end: datetime.datetime | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_start or max_start:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("start"),
                gte=min_start.isoformat() if min_start else None,
                lte=max_start.isoformat() if max_start else None,
            )
        )
    if min_end or max_end:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("end"),
                gte=min_end.isoformat() if min_end else None,
                lte=max_end.isoformat() if max_end else None,
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
