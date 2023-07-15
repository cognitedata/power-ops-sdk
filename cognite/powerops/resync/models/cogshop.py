from pydantic import Field

from cognite.powerops.clients.data_classes import (
    InputTimeSeriesMappingApply,
    OutputMappingApply,
    ScenarioTemplateApply,
    ValueTransformationApply,
)

from ._base import AssetModel, DataModel, Model, ResourceType
from .cdf_resources import CDFFile, CDFSequence


class BaseMapping(ResourceType):
    watercourse_name: str
    mapping: list[CDFSequence] = Field(default_factory=list)


class OutputDefinition(ResourceType):
    watercourse_name: str
    mapping: list[CDFSequence] = Field(default_factory=list)


class ShopFile(ResourceType):
    watercourse_name: str
    file: CDFFile


class CogShopCore(Model):
    shop_files: list[ShopFile] = Field(default_factory=list)


class CogShopDataModel(CogShopCore, DataModel):
    scenario_templates: list[ScenarioTemplateApply] = Field(default_factory=list)
    input_time_series_mappings: list[InputTimeSeriesMappingApply] = Field(default_factory=list)
    output_mappings: list[OutputMappingApply] = Field(default_factory=list)
    value_transformations: list[ValueTransformationApply] = Field(default_factory=list)


class CogShopAsset(CogShopCore, AssetModel):
    root_asset = None
    base_mappings: list[BaseMapping] = Field(default_factory=list)
    output_definitions: list[OutputDefinition] = Field(default_factory=list)
