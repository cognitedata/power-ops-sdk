from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.cogshop1.data_classes._core import DomainModel, DomainModelApply, TypeList

__all__ = ["FileRef", "FileRefApply", "FileRefList"]


class FileRef(DomainModel):
    space: ClassVar[str] = "cogShop"
    file_external_id: Optional[str] = Field(None, alias="fileExternalId")
    type: Optional[str] = None


class FileRefApply(DomainModelApply):
    space: ClassVar[str] = "cogShop"
    file_external_id: str
    type: str

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("cogShop", "FileRef"),
            properties={
                "fileExternalId": self.file_external_id,
                "type": self.type,
            },
        )
        sources.append(source)

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=sources,
        )
        nodes = [this_node]
        edges = []
        cache.add(self.external_id)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class FileRefList(TypeList[FileRef]):
    _NODE = FileRef
