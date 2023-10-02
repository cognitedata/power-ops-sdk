from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.cogshop1.data_classes import (
    ModelTemplate,
    ModelTemplateApply,
    ModelTemplateApplyList,
    ModelTemplateList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class ModelTemplateBaseMappingsAPI:
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

    def list(self, model_template_id: str | list[str] | None = None, limit=DEFAULT_LIMIT_READ) -> dm.EdgeList:
        f = dm.filters
        filters = []
        is_edge_type = f.Equals(
            ["edge", "type"],
            {"space": "cogShop", "externalId": "ModelTemplate.baseMappings"},
        )
        filters.append(is_edge_type)
        if model_template_id:
            model_template_ids = [model_template_id] if isinstance(model_template_id, str) else model_template_id
            is_model_templates = f.In(
                ["edge", "startNode"],
                [{"space": "cogShop", "externalId": ext_id} for ext_id in model_template_ids],
            )
            filters.append(is_model_templates)

        return self._client.data_modeling.instances.list("edge", limit=limit, filter=f.And(*filters))


class ModelTemplateAPI(TypeAPI[ModelTemplate, ModelTemplateApply, ModelTemplateList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ModelTemplate,
            class_apply_type=ModelTemplateApply,
            class_list=ModelTemplateList,
        )
        self.view_id = view_id
        self.base_mappings = ModelTemplateBaseMappingsAPI(client)

    def apply(
        self, model_template: ModelTemplateApply | Sequence[ModelTemplateApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(model_template, ModelTemplateApply):
            instances = model_template.to_instances_apply()
        else:
            instances = ModelTemplateApplyList(model_template).to_instances_apply()
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

    def list(
        self,
        version: str | list[str] | None = None,
        version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        watercourse: str | list[str] | None = None,
        watercourse_prefix: str | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> ModelTemplateList:
        filter_ = _create_filter(
            self.view_id,
            version,
            version_prefix,
            shop_version,
            shop_version_prefix,
            watercourse,
            watercourse_prefix,
            source,
            source_prefix,
            external_id_prefix,
            filter,
        )

        model_templates = self._list(limit=limit, filter=filter_)

        if retrieve_edges:
            base_mapping_edges = self.base_mappings.list(model_templates.as_external_ids(), limit=-1)
            self._set_base_mappings(model_templates, base_mapping_edges)

        return model_templates

    @staticmethod
    def _set_base_mappings(model_templates: Sequence[ModelTemplate], base_mapping_edges: Sequence[dm.Edge]):
        edges_by_start_node: dict[tuple, list] = defaultdict(list)
        for edge in base_mapping_edges:
            edges_by_start_node[edge.start_node.as_tuple()].append(edge)

        for model_template in model_templates:
            node_id = model_template.id_tuple()
            if node_id in edges_by_start_node:
                model_template.base_mappings = [edge.end_node.external_id for edge in edges_by_start_node[node_id]]


def _create_filter(
    view_id: dm.ViewId,
    version: str | list[str] | None = None,
    version_prefix: str | None = None,
    shop_version: str | list[str] | None = None,
    shop_version_prefix: str | None = None,
    watercourse: str | list[str] | None = None,
    watercourse_prefix: str | None = None,
    source: str | list[str] | None = None,
    source_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if version and isinstance(version, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("version"), value=version))
    if version and isinstance(version, list):
        filters.append(dm.filters.In(view_id.as_property_ref("version"), values=version))
    if version_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("version"), value=version_prefix))
    if shop_version and isinstance(shop_version, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopVersion"), value=shop_version))
    if shop_version and isinstance(shop_version, list):
        filters.append(dm.filters.In(view_id.as_property_ref("shopVersion"), values=shop_version))
    if shop_version_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("shopVersion"), value=shop_version_prefix))
    if watercourse and isinstance(watercourse, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("watercourse"), value=watercourse))
    if watercourse and isinstance(watercourse, list):
        filters.append(dm.filters.In(view_id.as_property_ref("watercourse"), values=watercourse))
    if watercourse_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("watercourse"), value=watercourse_prefix))
    if source and isinstance(source, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("source"), value=source))
    if source and isinstance(source, list):
        filters.append(dm.filters.In(view_id.as_property_ref("source"), values=source))
    if source_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("source"), value=source_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
