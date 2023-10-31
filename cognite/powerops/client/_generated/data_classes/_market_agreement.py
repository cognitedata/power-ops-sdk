from __future__ import annotations

import datetime
from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = [
    "MarketAgreement",
    "MarketAgreementApply",
    "MarketAgreementList",
    "MarketAgreementApplyList",
    "MarketAgreementFields",
    "MarketAgreementTextFields",
]


MarketAgreementTextFields = Literal["m_rid", "type"]
MarketAgreementFields = Literal["m_rid", "type", "created_timestamp"]

_MARKETAGREEMENT_PROPERTIES_BY_FIELD = {
    "m_rid": "mRID",
    "type": "type",
    "created_timestamp": "createdTimestamp",
}


class MarketAgreement(DomainModel):
    space: str = "power-ops"
    m_rid: Optional[str] = Field(None, alias="mRID")
    type: Optional[str] = None
    created_timestamp: Optional[datetime.datetime] = Field(None, alias="createdTimestamp")

    def as_apply(self) -> MarketAgreementApply:
        return MarketAgreementApply(
            external_id=self.external_id,
            m_rid=self.m_rid,
            type=self.type,
            created_timestamp=self.created_timestamp,
        )


class MarketAgreementApply(DomainModelApply):
    space: str = "power-ops"
    m_rid: Optional[str] = Field(None, alias="mRID")
    type: Optional[str] = None
    created_timestamp: Optional[datetime.datetime] = Field(None, alias="createdTimestamp")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.m_rid is not None:
            properties["mRID"] = self.m_rid
        if self.type is not None:
            properties["type"] = self.type
        if self.created_timestamp is not None:
            properties["createdTimestamp"] = self.created_timestamp.isoformat(timespec="milliseconds")
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "MarketAgreement"),
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


class MarketAgreementList(TypeList[MarketAgreement]):
    _NODE = MarketAgreement

    def as_apply(self) -> MarketAgreementApplyList:
        return MarketAgreementApplyList([node.as_apply() for node in self.data])


class MarketAgreementApplyList(TypeApplyList[MarketAgreementApply]):
    _NODE = MarketAgreementApply
