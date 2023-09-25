from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from cognite.powerops.client.data_classes import (
    GeneratorApply,
    PlantApply,
    PriceAreaApply,
    ReservoirApply,
    WatercourseApply,
)
from cognite.powerops.client.powerops_client import PowerOpsClient
from cognite.powerops.resync.models.base import CDFSequence, DataModel, PowerOpsGraphQLModel, T_Model

from .graphql_schemas import GRAPHQL_MODELS


class ProductionModelDM(DataModel):
    graph_ql: ClassVar[PowerOpsGraphQLModel] = GRAPHQL_MODELS["production"]
    cdf_sequences: list[CDFSequence] = Field(default_factory=list)
    price_areas: list[PriceAreaApply] = Field(default_factory=list)
    watercourses: list[WatercourseApply] = Field(default_factory=list)
    plants: list[PlantApply] = Field(default_factory=list)
    generators: list[GeneratorApply] = Field(default_factory=list)
    reservoirs: list[ReservoirApply] = Field(default_factory=list)

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
        sequences = client.cdf.sequences.retrieve_multiple(external_ids=sequence_external_ids, ignore_unknown_ids=True)

        watercourse_shop_by_external_id = {w.external_id: w.as_apply() for w in watercourse_shop}
        for watercourse in watercourses:
            watercourse.shop = watercourse_shop_by_external_id[watercourse.external_id]

        sequence_by_external_ids = {sequence.external_id: sequence for sequence in sequences}
        for generator in generators:
            generator.generator_efficiency_curve = sequence_by_external_ids[generator.generator_efficiency_curve]
            generator.turbine_efficiency_curve = sequence_by_external_ids[generator.turbine_efficiency_curve]

        return cls(
            price_areas=list(price_areas.as_apply()),
            watercourses=list(watercourse.as_apply()),
            plants=list(plants.as_apply()),
            generators=list(generators.as_apply()),
            reservoirs=list(reservoirs.as_apply()),
            cdf_sequences=[CDFSequence(sequence=sequence) for sequence in sequences],
        )

    def standardize(self) -> None:
        raise NotImplementedError()
