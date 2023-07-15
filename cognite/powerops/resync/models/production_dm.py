from __future__ import annotations

from pydantic import Field

from cognite.powerops.clients.data_classes import (
    GeneratorApply,
    PlantApply,
    PriceAreaApply,
    ReservoirApply,
    WatercourseApply,
)
from cognite.powerops.resync.models.base import DataModel
from cognite.powerops.resync.models.cdf_resources import CDFSequence


class ProductionModelDM(DataModel):
    cdf_sequences: list[CDFSequence] = Field(default_factory=list)
    price_areas: list[PriceAreaApply] = Field(default_factory=list)
    watercourses: list[WatercourseApply] = Field(default_factory=list)
    plants: list[PlantApply] = Field(default_factory=list)
    generators: list[GeneratorApply] = Field(default_factory=list)
    reservoirs: list[ReservoirApply] = Field(default_factory=list)
