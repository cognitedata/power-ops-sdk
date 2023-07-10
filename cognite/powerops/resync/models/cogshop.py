from pydantic import Field

from cognite.powerops.clients.cogshop.data_classes import (
    FileRefApply,
    MappingApply,
    ModelTemplateApply,
    TransformationApply,
)

from ._base import CDFSequence, DataModel, Type


class OutputDefinition(Type):
    watercourse_name: str
    mapping: list[CDFSequence] = Field(default_factory=list)


class CogShopModel(DataModel):
    output_definitions: list[OutputDefinition] = Field(default_factory=list)
    model_templates: list[ModelTemplateApply] = Field(default_factory=list)
    mappings: list[MappingApply] = Field(default_factory=list)
    transformations: list[TransformationApply] = Field(default_factory=list)
    file_refs: list[FileRefApply] = Field(default_factory=list)
