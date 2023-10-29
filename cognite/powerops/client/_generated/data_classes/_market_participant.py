from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = [
    "MarketParticipant",
    "MarketParticipantApply",
    "MarketParticipantList",
    "MarketParticipantApplyList",
    "MarketParticipantFields",
    "MarketParticipantTextFields",
]


MarketParticipantTextFields = Literal["m_rid", "role"]
MarketParticipantFields = Literal["m_rid", "role"]

_MARKETPARTICIPANT_PROPERTIES_BY_FIELD = {
    "m_rid": "mRID",
    "role": "role",
}


class MarketParticipant(DomainModel):
    space: str = "power-ops"
    m_rid: Optional[str] = Field(None, alias="mRID")
    role: Optional[str] = None

    def as_apply(self) -> MarketParticipantApply:
        return MarketParticipantApply(
            external_id=self.external_id,
            m_rid=self.m_rid,
            role=self.role,
        )


class MarketParticipantApply(DomainModelApply):
    space: str = "power-ops"
    m_rid: Optional[str] = Field(None, alias="mRID")
    role: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.m_rid is not None:
            properties["mRID"] = self.m_rid
        if self.role is not None:
            properties["role"] = self.role
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "MarketParticipant"),
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


class MarketParticipantList(TypeList[MarketParticipant]):
    _NODE = MarketParticipant

    def as_apply(self) -> MarketParticipantApplyList:
        return MarketParticipantApplyList([node.as_apply() for node in self.data])


class MarketParticipantApplyList(TypeApplyList[MarketParticipantApply]):
    _NODE = MarketParticipantApply
