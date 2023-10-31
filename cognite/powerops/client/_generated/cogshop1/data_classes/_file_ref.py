from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = ["FileRef", "FileRefApply", "FileRefList", "FileRefApplyList", "FileRefFields", "FileRefTextFields"]


FileRefTextFields = Literal["type", "file_external_id"]
FileRefFields = Literal["type", "file_external_id"]

_FILEREF_PROPERTIES_BY_FIELD = {
    "type": "type",
    "file_external_id": "fileExternalId",
}


class FileRef(DomainModel):
    space: str = "cogShop"
    type: Optional[str] = None
    file_external_id: Optional[str] = Field(None, alias="fileExternalId")

    def as_apply(self) -> FileRefApply:
        return FileRefApply(
            external_id=self.external_id,
            type=self.type,
            file_external_id=self.file_external_id,
        )


class FileRefApply(DomainModelApply):
    space: str = "cogShop"
    type: str
    file_external_id: str = Field(alias="fileExternalId")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.type is not None:
            properties["type"] = self.type
        if self.file_external_id is not None:
            properties["fileExternalId"] = self.file_external_id
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("cogShop", "FileRef"),
                properties=properties,
            )
            sources.append(source)
        if sources:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=sources,
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class FileRefList(TypeList[FileRef]):
    _NODE = FileRef

    def as_apply(self) -> FileRefApplyList:
        return FileRefApplyList([node.as_apply() for node in self.data])


class FileRefApplyList(TypeApplyList[FileRefApply]):
    _NODE = FileRefApply
