from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.clients.data_classes._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from cognite.powerops.clients.data_classes._bid_matrix_generators import BidMatrixGeneratorApply
    from cognite.powerops.clients.data_classes._day_ahead_bids import DayAheadBidApply
    from cognite.powerops.clients.data_classes._shop_transformations import ShopTransformationApply

__all__ = ["DayAheadProces", "DayAheadProcesApply", "DayAheadProcesList"]


class DayAheadProces(DomainModel):
    space: ClassVar[str] = "power-ops"
    bid: Optional[str] = None
    bid_matrix_generator_config: list[str] = Field([], alias="bidMatrixGeneratorConfig")
    name: Optional[str] = None
    shop: Optional[str] = None


class DayAheadProcesApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    bid: Optional[Union["DayAheadBidApply", str]] = Field(None, repr=False)
    bid_matrix_generator_config: list[Union["BidMatrixGeneratorApply", str]] = Field(default_factory=list, repr=False)
    name: Optional[str] = None
    shop: Optional[Union["ShopTransformationApply", str]] = Field(None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "DayAheadProcess"),
            properties={
                "bid": {
                    "space": "power-ops",
                    "externalId": self.bid if isinstance(self.bid, str) else self.bid.external_id,
                },
                "name": self.name,
                "shop": {
                    "space": "power-ops",
                    "externalId": self.shop if isinstance(self.shop, str) else self.shop.external_id,
                },
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

        for bid_matrix_generator_config in self.bid_matrix_generator_config:
            edge = self._create_bid_matrix_generator_config_edge(bid_matrix_generator_config)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(bid_matrix_generator_config, DomainModelApply):
                instances = bid_matrix_generator_config._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.bid, DomainModelApply):
            instances = self.bid._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.shop, DomainModelApply):
            instances = self.shop._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return InstancesApply(nodes, edges)

    def _create_bid_matrix_generator_config_edge(
        self, bid_matrix_generator_config: Union[str, "BidMatrixGeneratorApply"]
    ) -> dm.EdgeApply:
        if isinstance(bid_matrix_generator_config, str):
            end_node_ext_id = bid_matrix_generator_config
        elif isinstance(bid_matrix_generator_config, DomainModelApply):
            end_node_ext_id = bid_matrix_generator_config.external_id
        else:
            raise TypeError(f"Expected str or BidMatrixGeneratorApply, got {type(bid_matrix_generator_config)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "DayAheadProcess.bidMatrixGeneratorConfig"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class DayAheadProcesList(TypeList[DayAheadProces]):
    _NODE = DayAheadProces
