from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._market_participant import MarketParticipantApply
    from ._mba_domain import MBADomainApply

__all__ = ["ReserveBid", "ReserveBidApply", "ReserveBidList", "ReserveBidApplyList"]


class ReserveBid(DomainModel):
    space: ClassVar[str] = "power-ops"
    m_rid: Optional[str] = Field(None, alias="mRID")
    revision_number: Optional[str] = Field(None, alias="revisionNumber")
    type: Optional[str] = None
    process_type: Optional[str] = Field(None, alias="processType")
    sender: Optional[str] = None
    receiver: Optional[str] = None
    created_date_time: Optional[datetime.datetime] = Field(None, alias="createdDateTime")
    domain: Optional[str] = None
    subject: Optional[str] = None

    def as_apply(self) -> ReserveBidApply:
        return ReserveBidApply(
            external_id=self.external_id,
            m_rid=self.m_rid,
            revision_number=self.revision_number,
            type=self.type,
            process_type=self.process_type,
            sender=self.sender,
            receiver=self.receiver,
            created_date_time=self.created_date_time,
            domain=self.domain,
            subject=self.subject,
        )


class ReserveBidApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    m_rid: Optional[str] = None
    revision_number: Optional[str] = None
    type: Optional[str] = None
    process_type: Optional[str] = None
    sender: Union[MarketParticipantApply, str, None] = Field(None, repr=False)
    receiver: Union[MarketParticipantApply, str, None] = Field(None, repr=False)
    created_date_time: Optional[datetime.datetime] = None
    domain: Union[MBADomainApply, str, None] = Field(None, repr=False)
    subject: Union[MarketParticipantApply, str, None] = Field(None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.m_rid is not None:
            properties["mRID"] = self.m_rid
        if self.revision_number is not None:
            properties["revisionNumber"] = self.revision_number
        if self.type is not None:
            properties["type"] = self.type
        if self.process_type is not None:
            properties["processType"] = self.process_type
        if self.sender is not None:
            properties["sender"] = {
                "space": "power-ops",
                "externalId": self.sender if isinstance(self.sender, str) else self.sender.external_id,
            }
        if self.receiver is not None:
            properties["receiver"] = {
                "space": "power-ops",
                "externalId": self.receiver if isinstance(self.receiver, str) else self.receiver.external_id,
            }
        if self.created_date_time is not None:
            properties["createdDateTime"] = self.created_date_time.isoformat()
        if self.domain is not None:
            properties["domain"] = {
                "space": "power-ops",
                "externalId": self.domain if isinstance(self.domain, str) else self.domain.external_id,
            }
        if self.subject is not None:
            properties["subject"] = {
                "space": "power-ops",
                "externalId": self.subject if isinstance(self.subject, str) else self.subject.external_id,
            }
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "ReserveBid"),
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

        if isinstance(self.sender, DomainModelApply):
            instances = self.sender._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.receiver, DomainModelApply):
            instances = self.receiver._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.domain, DomainModelApply):
            instances = self.domain._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.subject, DomainModelApply):
            instances = self.subject._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class ReserveBidList(TypeList[ReserveBid]):
    _NODE = ReserveBid

    def as_apply(self) -> ReserveBidApplyList:
        return ReserveBidApplyList([node.as_apply() for node in self.data])


class ReserveBidApplyList(TypeApplyList[ReserveBidApply]):
    _NODE = ReserveBidApply
