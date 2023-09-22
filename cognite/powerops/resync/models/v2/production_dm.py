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
    def from_cdf(
        cls: type[T_Model], client: PowerOpsClient, fetch_metadata: bool = True, fetch_content: bool = False
    ) -> T_Model:
        raise NotImplementedError()

    def standardize(self) -> None:
        raise NotImplementedError()
