from __future__ import annotations
from pydantic import Field

from cognite.powerops.clients.data_classes import (
    ScenarioTemplateApply,
    ValueTransformationApply,
    OutputContainerApply,
    ScenarioMappingApply,
)
import cognite.powerops.cogshop1.data_classes as cogshop_v1
from .base import DataModel, Model
from .cdf_resources import CDFFile, CDFSequence

ExternalID = str


class CogShopCore(Model):
    shop_files: list[CDFFile] = Field(default_factory=list)


class CogShopDataModel(CogShopCore, DataModel):
    scenario_templates: list[ScenarioTemplateApply] = Field(default_factory=list)
    base_mappings: dict[ExternalID, ScenarioMappingApply] = Field(default_factory=dict)
    output_definitions: dict[ExternalID, OutputContainerApply] = Field(default_factory=dict)
    value_transformations: dict[ExternalID, ValueTransformationApply] = Field(default_factory=dict)


class CogShop1Asset(CogShopCore, DataModel):
    model_templates: dict[ExternalID, cogshop_v1.ModelTemplateApply] = Field(default_factory=dict)
    base_mappings: list[CDFSequence] = Field(default_factory=list)
    output_definitions: list[CDFSequence] = Field(default_factory=list)

    @classmethod
    def from_cdf(cls, client) -> "CogShop1Asset":
        # TODO: undetermined how to handle
        raise NotImplementedError()
