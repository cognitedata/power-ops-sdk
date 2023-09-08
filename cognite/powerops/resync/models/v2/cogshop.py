from __future__ import annotations

from pydantic import Field

from cognite.powerops.client.data_classes import (
    OutputContainerApply,
    ScenarioMappingApply,
    ScenarioTemplateApply,
    ValueTransformationApply,
)
from cognite.powerops.resync.models._shared_v1_v2.cogshop_model import CogShopCore, ExternalID
from cognite.powerops.resync.models.base import DataModel


class CogShopDataModel(CogShopCore, DataModel):
    scenario_templates: list[ScenarioTemplateApply] = Field(default_factory=list)
    base_mappings: dict[ExternalID, ScenarioMappingApply] = Field(default_factory=dict)
    output_definitions: dict[ExternalID, OutputContainerApply] = Field(default_factory=dict)
    value_transformations: dict[ExternalID, ValueTransformationApply] = Field(default_factory=dict)
