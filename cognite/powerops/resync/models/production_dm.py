from __future__ import annotations

from pydantic import Field

from cognite.powerops.clients.data_classes import (
    GeneratorApply,
    PlantApply,
    PriceAreaApply,
    ReservoirApply,
    WatercourseApply,
)
from cognite.powerops.resync.models._base import DataModel


class ProductionModelDM(DataModel):
    price_areas: list[PriceAreaApply] = Field(default_factory=list)
    watercourses: list[WatercourseApply] = Field(default_factory=list)
    plants: list[PlantApply] = Field(default_factory=list)
    generators: list[GeneratorApply] = Field(default_factory=list)
    reservoirs: list[ReservoirApply] = Field(default_factory=list)
