from __future__ import annotations

from typing import ClassVar

from cognite.client.data_classes.data_modeling import ContainerId
from pydantic import Field, field_validator

from cognite.powerops.client._generated.data_classes._core import DomainModelApply
from cognite.powerops.client.data_classes import (
    GeneratorApply,
    PlantApply,
    PriceAreaApply,
    ReservoirApply,
    WatercourseApply,
    WatercourseShopApply,
)
from cognite.powerops.client.powerops_client import PowerOpsClient
from cognite.powerops.resync.models.base import CDFSequence, DataModel, PowerOpsGraphQLModel, T_Model

from .graphql_schemas import GRAPHQL_MODELS


class ProductionModelDM(DataModel):
    graph_ql: ClassVar[PowerOpsGraphQLModel] = GRAPHQL_MODELS["production"]
    cls_by_container: ClassVar[dict[ContainerId, type[DomainModelApply]]] = {
        ContainerId("power-ops", "PriceArea"): PriceAreaApply,
        ContainerId("power-ops", "Watercourse"): WatercourseApply,
        ContainerId("power-ops", "Plant"): PlantApply,
        ContainerId("power-ops", "Generator"): GeneratorApply,
        ContainerId("power-ops", "Reservoir"): ReservoirApply,
        ContainerId("power-ops", "WatercourseShop"): WatercourseShopApply,
    }
    cdf_sequences: list[CDFSequence] = Field(default_factory=list)
    price_areas: list[PriceAreaApply] = Field(default_factory=list)
    watercourses: list[WatercourseApply] = Field(default_factory=list)
    plants: list[PlantApply] = Field(default_factory=list)
    generators: list[GeneratorApply] = Field(default_factory=list)
    reservoirs: list[ReservoirApply] = Field(default_factory=list)

    @field_validator("cdf_sequences", "price_areas", "watercourses", "plants", "generators", "reservoirs", mode="after")
    def ordering_list(cls, value: list) -> list:
        return sorted(value, key=lambda x: x.external_id)

    @classmethod
    def from_cdf(cls: type[T_Model], client: PowerOpsClient, data_set_external_id: str) -> T_Model:
        production = client.production
        price_areas = production.price_area.list(limit=-1)
        watercourses = production.watercourse.list(limit=-1)
        plants = production.plant.list(limit=-1)
        generators = production.generator.list(limit=-1)
        reservoirs = production.reservoir.list(limit=-1)
        watercourse_shop = production.watercourse_shop.list(limit=-1)
        sequence_external_ids = [
            external_id
            for generator in generators
            for external_id in [generator.generator_efficiency_curve, generator.turbine_efficiency_curve]
        ]
        if sequence_external_ids:
            sequences = client.cdf.sequences.retrieve_multiple(
                external_ids=sequence_external_ids, ignore_unknown_ids=True
            )
        else:
            sequences = []

        watercourse_shop_by_external_id = {w.external_id: w.as_apply() for w in watercourse_shop}
        for watercourse in watercourses:
            watercourse.shop = watercourse_shop_by_external_id[watercourse.shop]

        return cls(
            price_areas=list(price_areas.as_apply()),
            watercourses=list(watercourses.as_apply()),
            plants=list(plants.as_apply()),
            generators=list(generators.as_apply()),
            reservoirs=list(reservoirs.as_apply()),
            cdf_sequences=[CDFSequence(sequence=sequence) for sequence in sequences],
        )

    def standardize(self) -> None:
        self.cdf_sequences = self.ordering_list(self.cdf_sequences)
        self.price_areas = self.ordering_list(self.price_areas)
        self.watercourses = self.ordering_list(self.watercourses)
        self.plants = self.ordering_list(self.plants)
        self.generators = self.ordering_list(self.generators)
        self.reservoirs = self.ordering_list(self.reservoirs)
