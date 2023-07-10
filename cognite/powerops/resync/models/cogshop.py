from pydantic import Field

from ._base import CDFSequence, DataModel, Type


class OutputDefinition(Type):
    watercourse_name: str
    mapping: list[CDFSequence] = Field(default_factory=list)


class CogShopModel(DataModel):
    output_definitions: list[OutputDefinition] = Field(default_factory=list)
