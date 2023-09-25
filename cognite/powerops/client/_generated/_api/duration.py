from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import Duration, DurationApply, DurationApplyList, DurationList

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class DurationAPI(TypeAPI[Duration, DurationApply, DurationList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Duration,
            class_apply_type=DurationApply,
            class_list=DurationList,
        )
        self.view_id = view_id

    def apply(
        self, duration: DurationApply | Sequence[DurationApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(duration, DurationApply):
            instances = duration.to_instances_apply()
        else:
            instances = DurationApplyList(duration).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(DurationApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(DurationApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Duration:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> DurationList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Duration | DurationList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(
        self,
        min_duration: int | None = None,
        max_duration: int | None = None,
        unit: str | list[str] | None = None,
        unit_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> DurationList:
        filter_ = _create_filter(
            self.view_id,
            min_duration,
            max_duration,
            unit,
            unit_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    min_duration: int | None = None,
    max_duration: int | None = None,
    unit: str | list[str] | None = None,
    unit_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_duration or max_duration:
        filters.append(dm.filters.Range(view_id.as_property_ref("duration"), gte=min_duration, lte=max_duration))
    if unit and isinstance(unit, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("unit"), value=unit))
    if unit and isinstance(unit, list):
        filters.append(dm.filters.In(view_id.as_property_ref("unit"), values=unit))
    if unit_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("unit"), value=unit_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
