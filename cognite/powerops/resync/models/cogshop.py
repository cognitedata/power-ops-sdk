from cognite.powerops.clients.cogshop.data_classes import CaseApply, ModelTemplateApply, ScenarioApply

from ._base import DataModel


class CogShopModel(DataModel):
    model_templates: list[ModelTemplateApply]
    ScenarioApply: list[ScenarioApply]
    cases: list[CaseApply]
