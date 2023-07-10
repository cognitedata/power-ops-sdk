from pydantic import Field

from cognite.powerops.clients.cogshop.data_classes import CaseApply, ModelTemplateApply, ScenarioApply

from ._base import DataModel


class CogShopModel(DataModel):
    model_templates: list[ModelTemplateApply] = Field(default_factory=list)
    scenarios: list[ScenarioApply] = Field(default_factory=list)
    cases: list[CaseApply] = Field(default_factory=list)
