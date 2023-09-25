from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from ._core import DEFAULT_LIMIT_READ, TypeAPI
from cognite.powerops.client._generated.data_classes import (
    DateTransformation,
    DateTransformationApply,
    DateTransformationList,
    DateTransformationApplyList,
)


class DateTransformationAPI(TypeAPI[DateTransformation, DateTransformationApply, DateTransformationList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=DateTransformation,
            class_apply_type=DateTransformationApply,
            class_list=DateTransformationList,
        )
        self.view_id = view_id

    def apply(
        self, date_transformation: DateTransformationApply | Sequence[DateTransformationApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(date_transformation, DateTransformationApply):
            instances = date_transformation.to_instances_apply()
        else:
            instances = DateTransformationApplyList(date_transformation).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(DateTransformationApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(DateTransformationApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> DateTransformation:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> DateTransformationList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> DateTransformation | DateTransformationList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(
        self,
        transformation: str | list[str] | None = None,
        transformation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> DateTransformationList:
        filter_ = _create_filter(
            self.view_id,
            transformation,
            transformation_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    transformation: str | list[str] | None = None,
    transformation_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if transformation and isinstance(transformation, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("transformation"), value=transformation))
    if transformation and isinstance(transformation, list):
        filters.append(dm.filters.In(view_id.as_property_ref("transformation"), values=transformation))
    if transformation_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("transformation"), value=transformation_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
