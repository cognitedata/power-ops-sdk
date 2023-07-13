from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from ._bid_matrix_generators import BidMatrixGeneratorApply
    from ._day_ahead_bids import DayAheadBidApply
    from ._input_time_series_mappings import InputTimeSeriesMappingApply
    from ._shop_transformations import ShopTransformationApply

__all__ = ["DayAheadProces", "DayAheadProcesApply", "DayAheadProcesList"]


class DayAheadProces(DomainModel):
    space: ClassVar[str] = "power-ops"
    bid: Optional[str] = None
    bid_matrix_generator_config: Optional[str] = Field(None, alias="bidMatrixGeneratorConfig")
    incremental_mapping: list[str] = Field([], alias="incrementalMapping")
    name: Optional[str] = None
    shop: Optional[str] = None


class DayAheadProcesApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    bid: Optional[Union[str, "DayAheadBidApply"]] = Field(None, repr=False)
    bid_matrix_generator_config: Optional[Union[str, "BidMatrixGeneratorApply"]] = Field(None, repr=False)
    incremental_mapping: list[Union[str, "InputTimeSeriesMappingApply"]] = Field(default_factory=lambda: [], repr=False)
    name: Optional[str] = None
    shop: Optional[Union[str, "ShopTransformationApply"]] = Field(None, repr=False)

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
                "bidMatrixGeneratorConfig": {
                    "space": "power-ops",
                    "externalId": self.bid_matrix_generator_config
                    if isinstance(self.bid_matrix_generator_config, str)
                    else self.bid_matrix_generator_config.external_id,
                },
                "shop": {
                    "space": "power-ops",
                    "externalId": self.shop if isinstance(self.shop, str) else self.shop.external_id,
                },
            },
        )
        sources.append(source)

        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "Process"),
            properties={
                "name": self.name,
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

        for incremental_mapping in self.incremental_mapping:
            edge = self._create_incremental_mapping_edge(incremental_mapping)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(incremental_mapping, DomainModelApply):
                instances = incremental_mapping._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.bid, DomainModelApply):
            instances = self.bid._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.bid_matrix_generator_config, DomainModelApply):
            instances = self.bid_matrix_generator_config._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.shop, DomainModelApply):
            instances = self.shop._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return InstancesApply(nodes, edges)

    def _create_incremental_mapping_edge(
        self, incremental_mapping: Union[str, "InputTimeSeriesMappingApply"]
    ) -> dm.EdgeApply:
        if isinstance(incremental_mapping, str):
            end_node_ext_id = incremental_mapping
        elif isinstance(incremental_mapping, DomainModelApply):
            end_node_ext_id = incremental_mapping.external_id
        else:
            raise TypeError(f"Expected str or InputTimeSeriesMappingApply, got {type(incremental_mapping)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "DayAheadProcess.incrementalMapping"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class DayAheadProcesList(TypeList[DayAheadProces]):
    _NODE = DayAheadProces
