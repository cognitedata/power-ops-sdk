from __future__ import annotations

from itertools import product
from typing import Type, ClassVar

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import ContainerId
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
from cognite.powerops.cogshop1.data_classes._core import DomainModelApply as DomainModelApplyCogShop1
from ...clients import PowerOpsClient

ExternalID = str


class CogShopCore(Model):
    shop_files: list[CDFFile] = Field(default_factory=list)


class CogShopDataModel(CogShopCore, DataModel):
    scenario_templates: list[ScenarioTemplateApply] = Field(default_factory=list)
    base_mappings: dict[ExternalID, ScenarioMappingApply] = Field(default_factory=dict)
    output_definitions: dict[ExternalID, OutputContainerApply] = Field(default_factory=dict)
    value_transformations: dict[ExternalID, ValueTransformationApply] = Field(default_factory=dict)


class CogShop1Asset(CogShopCore, DataModel, protected_namespaces=()):
    cls_by_container: ClassVar[dict[ContainerId, Type[DomainModelApplyCogShop1]]] = {
        ContainerId("cogShop", "ModelTemplate"): cogshop_v1.ModelTemplateApply,
        ContainerId("cogShop", "Mapping"): cogshop_v1.MappingApply,
        ContainerId("cogShop", "FileRef"): cogshop_v1.FileRefApply,
        ContainerId("cogShop", "Transformation"): cogshop_v1.TransformationApply,
    }
    model_templates: dict[ExternalID, cogshop_v1.ModelTemplateApply] = Field(default_factory=dict)
    mappings: dict[ExternalID, cogshop_v1.MappingApply] = Field(default_factory=dict)
    transformations: dict[ExternalID, cogshop_v1.TransformationApply] = Field(default_factory=dict)
    base_mappings: list[CDFSequence] = Field(default_factory=list)
    output_definitions: list[CDFSequence] = Field(default_factory=list)

    @classmethod
    def from_cdf(cls, client: PowerOpsClient, data_set_external_id: str) -> CogShop1Asset:
        cog_shop = client.cog_shop1
        templates = cog_shop.model_templates.list(limit=-1)
        base_mappings = cog_shop.mappings.list(limit=-1)
        transformations = cog_shop.transformations.list(limit=-1)
        files = cog_shop.file_refs.list(limit=-1)

        transformation_by_id = {t.external_id: t for t in transformations}
        mappings_by_id = {}
        readme_fields = {"created_time", "deleted_time", "last_updated_time", "version"}
        for transformation in transformations:
            data = transformation.model_dump(exclude=readme_fields)
            apply = cogshop_v1.TransformationApply(**data)
            transformation_by_id[apply.external_id] = apply

        for mapping in base_mappings:
            data = mapping.model_dump(exclude=readme_fields)
            data["transformations"] = [transformation_by_id[t] for t in data["transformations"]]
            apply = cogshop_v1.MappingApply(**data)
            mappings_by_id[apply.external_id] = apply

        file_by_id = {f.external_id: f for f in files}
        model_templates = {}
        for template in templates:
            data = template.model_dump(exclude=readme_fields - {"version"})
            data["version"] = str(data["version"])
            if data["model"] in file_by_id:
                data["model"] = file_by_id[data["model"]].model_dump(exclude=readme_fields)
            data["base_mappings"] = [mappings_by_id[m] for m in data["base_mappings"]]
            apply = cogshop_v1.ModelTemplateApply(**data)
            model_templates[apply.external_id] = apply

        # There files and sequences are not linked to the model templates
        # (this should be done in the next version of Cogshop).
        watercourse_names = list({t.watercourse for t in model_templates.values()})
        sequence_ids = [
            f"SHOP_{name}{suffix}"
            for name, suffix in product(watercourse_names, ["_base_mapping", "_output_definition"])
        ]
        cdf_client: CogniteClient = client.cdf
        sequences = cdf_client.sequences.retrieve_multiple(external_ids=sequence_ids)

        base_mappings = [CDFSequence(sequence=s) for s in sequences if s.external_id.endswith("_base_mapping")]
        output_definitions = [
            CDFSequence(sequence=s) for s in sequences if s.external_id.endswith("_output_definition")
        ]

        files = cdf_client.files.list(
            limit=-1,
            source="PowerOps bootstrap",
            mime_type="text/plain",
        )
        shop_files = [CDFFile(meta=f) for f in files]

        return cls(
            model_templates=model_templates,
            mappings=mappings_by_id,
            transformations=transformation_by_id,
            base_mappings=base_mappings,
            output_definitions=output_definitions,
            shop_files=shop_files,
        )
