from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import (
    OutputMapping,
    OutputMappingApply,
    OutputMappingApplyList,
    OutputMappingList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class OutputMappingAPI(TypeAPI[OutputMapping, OutputMappingApply, OutputMappingList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=OutputMapping,
            class_apply_type=OutputMappingApply,
            class_list=OutputMappingList,
        )
        self.view_id = view_id

    def apply(
        self, output_mapping: OutputMappingApply | Sequence[OutputMappingApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(output_mapping, OutputMappingApply):
            instances = output_mapping.to_instances_apply()
        else:
            instances = OutputMappingApplyList(output_mapping).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(OutputMappingApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(OutputMappingApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> OutputMapping:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> OutputMappingList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> OutputMapping | OutputMappingList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(
        self,
        shop_object_type: str | list[str] | None = None,
        shop_object_type_prefix: str | None = None,
        shop_attribute_name: str | list[str] | None = None,
        shop_attribute_name_prefix: str | None = None,
        cdf_attribute_name: str | list[str] | None = None,
        cdf_attribute_name_prefix: str | None = None,
        unit: str | list[str] | None = None,
        unit_prefix: str | None = None,
        is_step: bool | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> OutputMappingList:
        filter_ = _create_filter(
            self.view_id,
            shop_object_type,
            shop_object_type_prefix,
            shop_attribute_name,
            shop_attribute_name_prefix,
            cdf_attribute_name,
            cdf_attribute_name_prefix,
            unit,
            unit_prefix,
            is_step,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    shop_object_type: str | list[str] | None = None,
    shop_object_type_prefix: str | None = None,
    shop_attribute_name: str | list[str] | None = None,
    shop_attribute_name_prefix: str | None = None,
    cdf_attribute_name: str | list[str] | None = None,
    cdf_attribute_name_prefix: str | None = None,
    unit: str | list[str] | None = None,
    unit_prefix: str | None = None,
    is_step: bool | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if shop_object_type and isinstance(shop_object_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopObjectType"), value=shop_object_type))
    if shop_object_type and isinstance(shop_object_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("shopObjectType"), values=shop_object_type))
    if shop_object_type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("shopObjectType"), value=shop_object_type_prefix))
    if shop_attribute_name and isinstance(shop_attribute_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopAttributeName"), value=shop_attribute_name))
    if shop_attribute_name and isinstance(shop_attribute_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("shopAttributeName"), values=shop_attribute_name))
    if shop_attribute_name_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("shopAttributeName"), value=shop_attribute_name_prefix)
        )
    if cdf_attribute_name and isinstance(cdf_attribute_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("cdfAttributeName"), value=cdf_attribute_name))
    if cdf_attribute_name and isinstance(cdf_attribute_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("cdfAttributeName"), values=cdf_attribute_name))
    if cdf_attribute_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("cdfAttributeName"), value=cdf_attribute_name_prefix))
    if unit and isinstance(unit, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("unit"), value=unit))
    if unit and isinstance(unit, list):
        filters.append(dm.filters.In(view_id.as_property_ref("unit"), values=unit))
    if unit_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("unit"), value=unit_prefix))
    if is_step and isinstance(is_step, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isStep"), value=is_step))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
