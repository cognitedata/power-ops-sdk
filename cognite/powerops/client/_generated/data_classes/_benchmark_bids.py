from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client._generated.data_classes._core import DomainModel, DomainModelApply, TypeList

if TYPE_CHECKING:
    from cognite.powerops.client._generated.data_classes._date_transformations import DateTransformationApply
    from cognite.powerops.client._generated.data_classes._nord_pool_markets import NordPoolMarketApply

__all__ = ["BenchmarkBid", "BenchmarkBidApply", "BenchmarkBidList"]


class BenchmarkBid(DomainModel):
    space: ClassVar[str] = "power-ops"
    date: list[str] = []
    market: Optional[str] = None
    name: Optional[str] = None


class BenchmarkBidApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    date: list[Union[DateTransformationApply, str]] = Field(default_factory=list, repr=False)
    market: Optional[Union[NordPoolMarketApply, str]] = Field(None, repr=False)
    name: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.market is not None:
            properties["market"] = {
                "space": "power-ops",
                "externalId": self.market if isinstance(self.market, str) else self.market.external_id,
            }
        if self.name is not None:
            properties["name"] = self.name
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

        for date in self.date:
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
