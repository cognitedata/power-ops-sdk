from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = ["Reason", "ReasonApply", "ReasonList", "ReasonApplyList", "ReasonFields", "ReasonTextFields"]


ReasonTextFields = Literal["code", "text"]
ReasonFields = Literal["code", "text"]

_REASON_PROPERTIES_BY_FIELD = {
    "code": "code",
    "text": "text",
}


class Reason(DomainModel):
    space: str = "power-ops"
    code: Optional[str] = None
    text: Optional[str] = None

    def as_apply(self) -> ReasonApply:
        return ReasonApply(
            external_id=self.external_id,
            code=self.code,
            text=self.text,
        )


class ReasonApply(DomainModelApply):
    space: str = "power-ops"
    code: Optional[str] = None
    text: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.code is not None:
            properties["code"] = self.code
        if self.text is not None:
            properties["text"] = self.text
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "Reason"),
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


class ReasonList(TypeList[Reason]):
    _NODE = Reason

    def as_apply(self) -> ReasonApplyList:
        return ReasonApplyList([node.as_apply() for node in self.data])


class ReasonApplyList(TypeApplyList[ReasonApply]):
    _NODE = ReasonApply
