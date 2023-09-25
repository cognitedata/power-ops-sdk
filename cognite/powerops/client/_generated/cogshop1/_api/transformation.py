from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.cogshop1.data_classes import (
    Transformation,
    TransformationApply,
    TransformationApplyList,
    TransformationList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class TransformationAPI(TypeAPI[Transformation, TransformationApply, TransformationList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Transformation,
            class_apply_type=TransformationApply,
            class_list=TransformationList,
        )
        self.view_id = view_id

    def apply(
        self, transformation: TransformationApply | Sequence[TransformationApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(transformation, TransformationApply):
            instances = transformation.to_instances_apply()
        else:
            instances = TransformationApplyList(transformation).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(TransformationApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(TransformationApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Transformation:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> TransformationList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Transformation | TransformationList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(
        self,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        arguments: str | list[str] | None = None,
        arguments_prefix: str | None = None,
        min_order: int | None = None,
        max_order: int | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TransformationList:
        filter_ = _create_filter(
            self.view_id,
            method,
            method_prefix,
            arguments,
            arguments_prefix,
            min_order,
            max_order,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    method: str | list[str] | None = None,
    method_prefix: str | None = None,
    arguments: str | list[str] | None = None,
    arguments_prefix: str | None = None,
    min_order: int | None = None,
    max_order: int | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if method and isinstance(method, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("method"), value=method))
    if method and isinstance(method, list):
        filters.append(dm.filters.In(view_id.as_property_ref("method"), values=method))
    if method_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("method"), value=method_prefix))
    if arguments and isinstance(arguments, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("arguments"), value=arguments))
    if arguments and isinstance(arguments, list):
        filters.append(dm.filters.In(view_id.as_property_ref("arguments"), values=arguments))
    if arguments_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("arguments"), value=arguments_prefix))
    if min_order or max_order:
        filters.append(dm.filters.Range(view_id.as_property_ref("order"), gte=min_order, lte=max_order))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
