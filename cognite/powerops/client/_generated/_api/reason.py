from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import Reason, ReasonApply, ReasonApplyList, ReasonList

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class ReasonAPI(TypeAPI[Reason, ReasonApply, ReasonList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Reason,
            class_apply_type=ReasonApply,
            class_list=ReasonList,
        )
        self.view_id = view_id

    def apply(self, reason: ReasonApply | Sequence[ReasonApply], replace: bool = False) -> dm.InstancesApplyResult:
        if isinstance(reason, ReasonApply):
            instances = reason.to_instances_apply()
        else:
            instances = ReasonApplyList(reason).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(ReasonApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(ReasonApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Reason:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ReasonList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Reason | ReasonList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(
        self,
        code: str | list[str] | None = None,
        code_prefix: str | None = None,
        text: str | list[str] | None = None,
        text_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ReasonList:
        filter_ = _create_filter(
            self.view_id,
            code,
            code_prefix,
            text,
            text_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    code: str | list[str] | None = None,
    code_prefix: str | None = None,
    text: str | list[str] | None = None,
    text_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if code and isinstance(code, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("code"), value=code))
    if code and isinstance(code, list):
        filters.append(dm.filters.In(view_id.as_property_ref("code"), values=code))
    if code_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("code"), value=code_prefix))
    if text and isinstance(text, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("text"), value=text))
    if text and isinstance(text, list):
        filters.append(dm.filters.In(view_id.as_property_ref("text"), values=text))
    if text_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("text"), value=text_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
