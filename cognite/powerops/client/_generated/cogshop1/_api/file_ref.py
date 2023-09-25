from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.cogshop1.data_classes import (
    FileRef,
    FileRefApply,
    FileRefApplyList,
    FileRefList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class FileRefAPI(TypeAPI[FileRef, FileRefApply, FileRefList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=FileRef,
            class_apply_type=FileRefApply,
            class_list=FileRefList,
        )
        self.view_id = view_id

    def apply(self, file_ref: FileRefApply | Sequence[FileRefApply], replace: bool = False) -> dm.InstancesApplyResult:
        if isinstance(file_ref, FileRefApply):
            instances = file_ref.to_instances_apply()
        else:
            instances = FileRefApplyList(file_ref).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(FileRefApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(FileRefApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> FileRef:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> FileRefList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> FileRef | FileRefList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(
        self,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        file_external_id: str | list[str] | None = None,
        file_external_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> FileRefList:
        filter_ = _create_filter(
            self.view_id,
            type,
            type_prefix,
            file_external_id,
            file_external_id_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    type: str | list[str] | None = None,
    type_prefix: str | None = None,
    file_external_id: str | list[str] | None = None,
    file_external_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if type and isinstance(type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("type"), value=type))
    if type and isinstance(type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("type"), values=type))
    if type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("type"), value=type_prefix))
    if file_external_id and isinstance(file_external_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("fileExternalId"), value=file_external_id))
    if file_external_id and isinstance(file_external_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("fileExternalId"), values=file_external_id))
    if file_external_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("fileExternalId"), value=file_external_id_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
