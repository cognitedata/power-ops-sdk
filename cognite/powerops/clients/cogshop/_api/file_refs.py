from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.clients.cogshop.data_classes import FileRef, FileRefApply, FileRefList

from ._core import TypeAPI


class FileRefsAPI(TypeAPI[FileRef, FileRefApply, FileRefList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("cogShop", "FileRef", "ada67a3568563c"),
            class_type=FileRef,
            class_apply_type=FileRefApply,
            class_list=FileRefList,
        )

    def apply(self, file_ref: FileRefApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = file_ref.to_instances_apply()
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

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> FileRefList:
        return self._list(limit=limit)
