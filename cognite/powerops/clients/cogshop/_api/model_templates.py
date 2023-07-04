from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Sequence, Tuple, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.clients.cogshop.data_classes import ModelTemplate, ModelTemplateApply, ModelTemplateList

from ._core import TypeAPI


class ModelTemplateMappingsAPI:
    def __init__(self, client: CogniteClient):
        self._client = client

    def retrieve(self, external_id: str | Sequence[str]) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cogShop", "externalId": "ModelTemplate.baseMappings"},
        )
        if isinstance(external_id, str):
            is_model_template = f.Equals(
                ["edge", "startNode"],
                {"space": "cogShop", "externalId": external_id},
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_model_template)
            )

        else:
            is_model_templates = f.In(
                ["edge", "startNode"],
                [{"space": "cogShop", "externalId": ext_id} for ext_id in external_id],
            )
            return self._client.data_modeling.instances.list(
                "edge", limit=-1, filter=f.And(is_edge_type, is_model_templates)
            )

    def list(self, limit=INSTANCES_LIST_LIMIT_DEFAULT) -> dm.EdgeList:
        f = dm.filters
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cogShop", "externalId": "ModelTemplate.baseMappings"},
        )
        return self._client.data_modeling.instances.list("edge", limit=limit, filter=is_edge_type)


class ModelTemplatesAPI(TypeAPI[ModelTemplate, ModelTemplateApply, ModelTemplateList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("cogShop", "ModelTemplate", "d5ec6bd344c88d"),
            class_type=ModelTemplate,
            class_apply_type=ModelTemplateApply,
            class_list=ModelTemplateList,
        )
        self.base_mappings = ModelTemplateMappingsAPI(client)

    def apply(self, model_template: ModelTemplateApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = model_template.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(ModelTemplateApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(ModelTemplateApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> ModelTemplate:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ModelTemplateList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> ModelTemplate | ModelTemplateList:
        if isinstance(external_id, str):
            model_template = self._retrieve((self.sources.space, external_id))

            base_mapping_edges = self.base_mappings.retrieve(external_id)
            model_template.base_mappings = [edge.end_node.external_id for edge in base_mapping_edges]

            return model_template
        else:
            model_templates = self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

            base_mapping_edges = self.base_mappings.retrieve(external_id)
            self._set_base_mappings(model_templates, base_mapping_edges)

            return model_templates

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> ModelTemplateList:
        model_templates = self._list(limit=limit)

        base_mapping_edges = self.base_mappings.list(limit=-1)
        self._set_base_mappings(model_templates, base_mapping_edges)

        return model_templates

    @staticmethod
    def _set_base_mappings(model_templates: Sequence[ModelTemplate], base_mapping_edges: Sequence[dm.Edge]):
        edges_by_start_node: Dict[Tuple, List] = defaultdict(list)
        for edge in base_mapping_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for model_template in model_templates:
            node_id = model_template.id_tuple()
            if node_id in edges_by_start_node:
                model_template.base_mappings = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]
