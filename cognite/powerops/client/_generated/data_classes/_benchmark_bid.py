from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._date_transformation import DateTransformationApply
    from ._nord_pool_market import NordPoolMarketApply

__all__ = [
    "BenchmarkBid",
    "BenchmarkBidApply",
    "BenchmarkBidList",
    "BenchmarkBidApplyList",
    "BenchmarkBidFields",
    "BenchmarkBidTextFields",
]


BenchmarkBidTextFields = Literal["name"]
BenchmarkBidFields = Literal["name"]

_BENCHMARKBID_PROPERTIES_BY_FIELD = {
    "name": "name",
}


class BenchmarkBid(DomainModel):
    space: str = "power-ops"
    name: Optional[str] = None
    market: Optional[str] = None
    date: Optional[list[str]] = None

    def as_apply(self) -> BenchmarkBidApply:
        return BenchmarkBidApply(
            external_id=self.external_id,
            name=self.name,
            market=self.market,
            date=self.date,
        )


class BenchmarkBidApply(DomainModelApply):
    space: str = "power-ops"
    name: Optional[str] = None
    market: Union[NordPoolMarketApply, str, None] = Field(None, repr=False)
    date: Union[list[DateTransformationApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.market is not None:
            properties["market"] = {
                "space": "power-ops",
                "externalId": self.market if isinstance(self.market, str) else self.market.external_id,
            }
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "BenchmarkBid"),
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

        for date in self.date or []:
            edge = self._create_date_edge(date)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(date, DomainModelApply):
                instances = date._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.market, DomainModelApply):
            instances = self.market._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_date_edge(self, date: Union[str, DateTransformationApply]) -> dm.EdgeApply:
        if isinstance(date, str):
            end_node_ext_id = date
        elif isinstance(date, DomainModelApply):
            end_node_ext_id = date.external_id
        else:
            raise TypeError(f"Expected str or DateTransformationApply, got {type(date)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "BenchmarkBid.date"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class BenchmarkBidList(TypeList[BenchmarkBid]):
    _NODE = BenchmarkBid

    def as_apply(self) -> BenchmarkBidApplyList:
        return BenchmarkBidApplyList([node.as_apply() for node in self.data])


class BenchmarkBidApplyList(TypeApplyList[BenchmarkBidApply]):
    _NODE = BenchmarkBidApply
