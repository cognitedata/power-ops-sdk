from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client.dm_client.data_classes._core import (
    CircularModelApply,
    DomainModel,
    InstancesApply,
    TypeList,
)

__all__ = ["FileRef", "FileRefApply", "FileRefList"]


class FileRef(DomainModel):
    space: ClassVar[str] = "cogShop"
    type: Optional[str] = None
    file_external_id: Optional[str] = Field(None, alias="fileExternalId")


class FileRefApply(CircularModelApply):
    space: ClassVar[str] = "cogShop"
    type: str
    file_external_id: str

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=[
                dm.NodeOrEdgeData(
                    source=dm.ContainerId("cogShop", "FileRef"),
                    properties={
                        "type": self.type,
                        "fileExternalId": self.file_external_id,
                    },
                ),
            ],
        )
        nodes = [this_node]
        edges = []

        return InstancesApply(nodes, edges)


class FileRefList(TypeList[FileRef]):
    _NODE = FileRef
