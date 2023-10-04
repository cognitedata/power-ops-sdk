from __future__ import annotations

from itertools import product
from typing import ClassVar

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import ContainerId
from pydantic import Field, field_validator

from cognite.powerops.client import PowerOpsClient
from cognite.powerops.client._generated.cogshop1.data_classes._core import DomainModelApply as DomainModelApplyCogShop1
from cognite.powerops.client.data_classes import cogshop1 as cogshop_v1
from cognite.powerops.resync.models._shared_v1_v2.cogshop_model import CogShopCore, ExternalID
from cognite.powerops.resync.models.base import CDFFile, CDFSequence, DataModel, PowerOpsGraphQLModel

from .graphql_schemas import cogshop1_graphql


class CogShop1Asset(CogShopCore, DataModel, protected_namespaces=()):
    cls_by_container: ClassVar[dict[ContainerId, type[DomainModelApplyCogShop1]]] = {
        ContainerId("cogShop", "ModelTemplate"): cogshop_v1.ModelTemplateApply,
        ContainerId("cogShop", "Mapping"): cogshop_v1.MappingApply,
        ContainerId("cogShop", "FileRef"): cogshop_v1.FileRefApply,
        ContainerId("cogShop", "Transformation"): cogshop_v1.TransformationApply,
        ContainerId("cogShop", "Scenario"): cogshop_v1.ScenarioApply,
        ContainerId("cogShop", "CommandsConfig"): cogshop_v1.CommandsConfigApply,
    }
    graph_ql: ClassVar[PowerOpsGraphQLModel] = cogshop1_graphql
    model_templates: dict[ExternalID, cogshop_v1.ModelTemplateApply] = Field(default_factory=dict)
    mappings: dict[ExternalID, cogshop_v1.MappingApply] = Field(default_factory=dict)
    transformations: dict[ExternalID, cogshop_v1.TransformationApply] = Field(default_factory=dict)
    base_mappings: list[CDFSequence] = Field(default_factory=list)
    output_definitions: list[CDFSequence] = Field(default_factory=list)
    scenarios: dict[ExternalID, cogshop_v1.ScenarioApply] = Field(default_factory=dict)
    commands_configs: dict[ExternalID, cogshop_v1.CommandsConfigApply] = Field(default_factory=dict)

    @field_validator("base_mappings", mode="after")
    def ordering_sequences(cls, value: list[CDFSequence]) -> list[CDFSequence]:
        return sorted(value, key=lambda x: x.external_id)

    @field_validator("model_templates", "mappings", "transformations", mode="after")
    def ordering_dict(cls, value: dict) -> dict:
        return {k: v for k, v in sorted(value.items(), key=lambda x: x[0])}

    @classmethod
    def from_cdf(cls, client: PowerOpsClient, data_set_external_id: str) -> CogShop1Asset:
        cog_shop = client.cog_shop1
        templates = cog_shop.model_template.list(limit=-1, source="resync")
        scenarios = cog_shop.scenario.list(limit=-1, source="resync")

        base_mapping_ids = list(
            {m for t in templates for m in t.base_mappings or []}
            | {m for s in scenarios for m in s.mappings_override or []}
        )

        if base_mapping_ids:
            base_mappings = cog_shop.mapping.retrieve(external_id=base_mapping_ids)
        else:
            base_mappings = []

        if transformation_ids := list({t for m in base_mappings for t in m.transformations or []}):
            transformations = cog_shop.transformation.retrieve(transformation_ids)
        else:
            transformations = []

        if file_ids := list({t.model for t in templates} | {f for s in scenarios for f in s.extra_files or []}):
            files = cog_shop.file_ref.retrieve(file_ids)
        else:
            files = []

        if command_ids := list({s.commands for s in scenarios}):
            commands_configs = cog_shop.commands_config.retrieve(command_ids)
        else:
            commands_configs = []

        transformation_by_id: dict[str, cogshop_v1.TransformationApply] = {
            t.external_id: t.as_apply() for t in transformations
        }

        mappings_by_id: dict[str, cogshop_v1.MappingApply] = {}
        for mapping in base_mappings:
            apply = mapping.as_apply()
            apply.transformations = sorted(
                (transformation_by_id[t] for t in apply.transformations or [] if t in transformation_by_id),
                key=lambda x: x.order,
            )
            mappings_by_id[apply.external_id] = apply

        command_configs_by_id = {commands.external_id: commands.as_apply() for commands in commands_configs}

        file_by_id: dict[str, cogshop_v1.FileRefApply] = {f.external_id: f.as_apply() for f in files}
        model_templates: dict[str, cogshop_v1.ModelTemplateApply] = {}
        for template in templates:
            apply = template.as_apply()
            if apply.model in file_by_id:
                apply.model = file_by_id[apply.model]
            apply.base_mappings = [mappings_by_id[m] for m in apply.base_mappings or [] if m in mappings_by_id]
            model_templates[apply.external_id] = apply

        scenarios_by_id = {}
        for scenario in scenarios:
            apply = scenario.as_apply()
            apply.mappings_override = [mappings_by_id[m] for m in apply.mappings_override or []]
            apply.commands = command_configs_by_id.get(apply.commands)
            apply.model_template = model_templates.get(apply.model_template)
            scenarios_by_id[apply.external_id] = apply

        # There files and sequences are not linked to the model templates
        # (this should be done in the next version of Cogshop).
        watercourse_names = list({t.watercourse for t in model_templates.values()})
        sequence_ids = [
            f"SHOP_{name}{suffix}"
            for name, suffix in product(watercourse_names, ["_base_mapping", "_output_definition"])
        ]
        cdf_client: CogniteClient = client.cdf
        if sequence_ids:
            sequences = cdf_client.sequences.retrieve_multiple(external_ids=sequence_ids)
        else:
            all_sequences = cdf_client.sequences.list(limit=-1, external_id_prefix="SHOP_")
            sequences = [
                s
                for s in all_sequences
                if s.external_id.endswith("_base_mapping") or s.external_id.endswith("_output_definition")
            ]

        base_mappings = [CDFSequence(sequence=s) for s in sequences if s.external_id.endswith("_base_mapping")]
        output_definitions = [
            CDFSequence(sequence=s) for s in sequences if s.external_id.endswith("_output_definition")
        ]

        files = cdf_client.files.list(limit=-1, source="PowerOps bootstrap", mime_type="text/plain")
        shop_files = [CDFFile(meta=f) for f in files]

        return cls(
            model_templates=model_templates,
            mappings=mappings_by_id,
            transformations=transformation_by_id,
            base_mappings=base_mappings,
            output_definitions=output_definitions,
            shop_files=shop_files,
            scenarios=scenarios_by_id,
            commands_configs=command_configs_by_id,
        )

    def standardize(self) -> None:
        super().standardize()
        self.base_mappings = self.ordering_sequences(self.base_mappings)
        self.output_definitions = self.ordering_sequences(self.output_definitions)
        self.model_templates = self.ordering_dict(self.model_templates)
        self.mappings = self.ordering_dict(self.mappings)
        self.transformations = self.ordering_dict(self.transformations)
        self.scenarios = self.ordering_dict(self.scenarios)
        self.commands_configs = self.ordering_dict(self.commands_configs)
        for scenario in self.scenarios.values():
            scenario.mappings_override = sorted(
                scenario.mappings_override or [], key=lambda x: x if isinstance(x, str) else x.external_id
            )

        for template in self.model_templates.values():
            template.base_mappings = sorted(
                template.base_mappings or [], key=lambda x: x if isinstance(x, str) else x.external_id
            )

        for mapping in self.mappings.values():
            mapping.transformations = sorted(
                mapping.transformations or [], key=lambda x: x[-1] if isinstance(x, str) else x.external_id
            )
