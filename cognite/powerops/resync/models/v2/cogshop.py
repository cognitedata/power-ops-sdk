from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from cognite.powerops import PowerOpsClient
from cognite.powerops.client.data_classes import (
    OutputContainerApply,
    ScenarioMappingApply,
    ScenarioTemplateApply,
    ValueTransformationApply,
)
from cognite.powerops.resync.models._shared_v1_v2.cogshop_model import CogShopCore, ExternalID
from cognite.powerops.resync.models.base import DataModel, PowerOpsGraphQLModel, T_Model

from .graphql_schemas import GRAPHQL_MODELS


class CogShopDataModel(CogShopCore, DataModel):
    graph_ql: ClassVar[PowerOpsGraphQLModel] = GRAPHQL_MODELS["cogshop"]
    scenario_templates: list[ScenarioTemplateApply] = Field(default_factory=list)
    base_mappings: dict[ExternalID, ScenarioMappingApply] = Field(default_factory=dict)
    output_definitions: dict[ExternalID, OutputContainerApply] = Field(default_factory=dict)
    value_transformations: dict[ExternalID, ValueTransformationApply] = Field(default_factory=dict)

    @classmethod
    def from_cdf(cls: type[T_Model], client: PowerOpsClient, data_set_external_id: str) -> T_Model:
        raise NotImplementedError()
