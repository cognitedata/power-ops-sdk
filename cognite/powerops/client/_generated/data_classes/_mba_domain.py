from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = [
    "MBADomain",
    "MBADomainApply",
    "MBADomainList",
    "MBADomainApplyList",
    "MBADomainFields",
    "MBADomainTextFields",
]


MBADomainTextFields = Literal["m_rid"]
MBADomainFields = Literal["m_rid"]

_MBADOMAIN_PROPERTIES_BY_FIELD = {
    "m_rid": "mRID",
}


class MBADomain(DomainModel):
    space: str = "power-ops"
    m_rid: Optional[str] = Field(None, alias="mRID")

    def as_apply(self) -> MBADomainApply:
        return MBADomainApply(
            external_id=self.external_id,
            m_rid=self.m_rid,
        )


class MBADomainApply(DomainModelApply):
    space: str = "power-ops"
    m_rid: Optional[str] = Field(None, alias="mRID")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.m_rid is not None:
            properties["mRID"] = self.m_rid
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "MBADomain"),
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


class MBADomainList(TypeList[MBADomain]):
    _NODE = MBADomain

    def as_apply(self) -> MBADomainApplyList:
        return MBADomainApplyList([node.as_apply() for node in self.data])


class MBADomainApplyList(TypeApplyList[MBADomainApply]):
    _NODE = MBADomainApply
