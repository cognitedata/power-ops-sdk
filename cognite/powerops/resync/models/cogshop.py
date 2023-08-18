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
from ...cogshop1 import CogShop1Client

ExternalID = str


class CogShopCore(Model):
    shop_files: list[CDFFile] = Field(default_factory=list)


class CogShopDataModel(CogShopCore, DataModel):
    scenario_templates: list[ScenarioTemplateApply] = Field(default_factory=list)
    base_mappings: dict[ExternalID, ScenarioMappingApply] = Field(default_factory=dict)
    output_definitions: dict[ExternalID, OutputContainerApply] = Field(default_factory=dict)
    value_transformations: dict[ExternalID, ValueTransformationApply] = Field(default_factory=dict)


class CogShop1Asset(CogShopCore, DataModel, protected_namespaces=()):
    model_templates: dict[ExternalID, cogshop_v1.ModelTemplateApply] = Field(default_factory=dict)
    base_mappings: list[CDFSequence] = Field(default_factory=list)
    output_definitions: list[CDFSequence] = Field(default_factory=list)

    @classmethod
    def from_cdf(
        cls, client: CogShop1Client, fetch_metadata: bool = True, fetch_content: bool = False
    ) -> "CogShop1Asset":
        templates = client.model_templates.list(limit=-1)
        base_mapping_ids = list({mapping for t in templates for mapping in t.base_mappings})
        base_mappings = client.mappings.retrieve(base_mapping_ids)
        transformations_ids = list({t for m in base_mappings for t in m.transformations})
        transformations = client.transformations.retrieve(transformations_ids)
        file_ids = list({t.model for t in templates})
        files = client.file_refs.retrieve(file_ids)
        transformation_by_id = {t.external_id: t for t in transformations}
        mappings_by_id = {}
        readme_fields = {"created_time", "deleted_time", "last_updated_time", "version"}
        for mapping in base_mappings:
            data = mapping.model_dump(exclude=readme_fields)
            data["transformations"] = [
                transformation_by_id[t].model_dump(exclude=readme_fields) for t in data["transformations"]
            ]
            apply = cogshop_v1.MappingApply(**data)
            mappings_by_id[apply.external_id] = apply

        file_by_id = {f.external_id: f for f in files}
        model_templates = {}
        for template in templates:
            data = template.model_dump(exclude=readme_fields - {"version"})
            data["version"] = str(data["version"])
            data["model"] = file_by_id[data["model"]].model_dump(exclude=readme_fields)
            data["base_mappings"] = [mappings_by_id[m] for m in data["base_mappings"]]
            apply = cogshop_v1.ModelTemplateApply(**data)
            model_templates[apply.external_id] = apply

        return cls(
            model_templates=model_templates,
        )
