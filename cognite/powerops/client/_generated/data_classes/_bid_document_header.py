from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = [
    "BidDocumentHeader",
    "BidDocumentHeaderApply",
    "BidDocumentHeaderList",
    "BidDocumentHeaderApplyList",
    "BidDocumentHeaderFields",
    "BidDocumentHeaderTextFields",
]


BidDocumentHeaderTextFields = Literal[
    "document_type_name", "process_type_name", "tso", "owner", "created_date_time", "country", "origin"
]
BidDocumentHeaderFields = Literal[
    "document_type_name", "process_type_name", "tso", "owner", "created_date_time", "country", "origin"
]

_BIDDOCUMENTHEADER_PROPERTIES_BY_FIELD = {
    "document_type_name": "DocumentTypeName",
    "process_type_name": "ProcessTypeName",
    "tso": "TSO",
    "owner": "Owner",
    "created_date_time": "CreatedDateTime",
    "country": "Country",
    "origin": "Origin",
}


class BidDocumentHeader(DomainModel):
    space: str = "power-ops"
    document_type_name: Optional[list[str]] = Field(None, alias="DocumentTypeName")
    process_type_name: Optional[list[str]] = Field(None, alias="ProcessTypeName")
    tso: Optional[list[str]] = Field(None, alias="TSO")
    owner: Optional[list[str]] = Field(None, alias="Owner")
    created_date_time: Optional[list[str]] = Field(None, alias="CreatedDateTime")
    country: Optional[list[str]] = Field(None, alias="Country")
    origin: Optional[list[str]] = Field(None, alias="Origin")

    def as_apply(self) -> BidDocumentHeaderApply:
        return BidDocumentHeaderApply(
            external_id=self.external_id,
            document_type_name=self.document_type_name,
            process_type_name=self.process_type_name,
            tso=self.tso,
            owner=self.owner,
            created_date_time=self.created_date_time,
            country=self.country,
            origin=self.origin,
        )


class BidDocumentHeaderApply(DomainModelApply):
    space: str = "power-ops"
    document_type_name: Optional[list[str]] = Field(None, alias="DocumentTypeName")
    process_type_name: Optional[list[str]] = Field(None, alias="ProcessTypeName")
    tso: Optional[list[str]] = Field(None, alias="TSO")
    owner: Optional[list[str]] = Field(None, alias="Owner")
    created_date_time: Optional[list[str]] = Field(None, alias="CreatedDateTime")
    country: Optional[list[str]] = Field(None, alias="Country")
    origin: Optional[list[str]] = Field(None, alias="Origin")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.document_type_name is not None:
            properties["DocumentTypeName"] = self.document_type_name
        if self.process_type_name is not None:
            properties["ProcessTypeName"] = self.process_type_name
        if self.tso is not None:
            properties["TSO"] = self.tso
        if self.owner is not None:
            properties["Owner"] = self.owner
        if self.created_date_time is not None:
            properties["CreatedDateTime"] = self.created_date_time
        if self.country is not None:
            properties["Country"] = self.country
        if self.origin is not None:
            properties["Origin"] = self.origin
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "BidDocumentHeader"),
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


class BidDocumentHeaderList(TypeList[BidDocumentHeader]):
    _NODE = BidDocumentHeader

    def as_apply(self) -> BidDocumentHeaderApplyList:
        return BidDocumentHeaderApplyList([node.as_apply() for node in self.data])


class BidDocumentHeaderApplyList(TypeApplyList[BidDocumentHeaderApply]):
    _NODE = BidDocumentHeaderApply