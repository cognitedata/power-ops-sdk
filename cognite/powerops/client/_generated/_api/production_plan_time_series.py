from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import (
    ProductionPlanTimeSeries,
    ProductionPlanTimeSeriesApply,
    ProductionPlanTimeSeriesApplyList,
    ProductionPlanTimeSeriesList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class ProductionPlanTimeSeriesAPI(
    TypeAPI[ProductionPlanTimeSeries, ProductionPlanTimeSeriesApply, ProductionPlanTimeSeriesList]
):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ProductionPlanTimeSeries,
            class_apply_type=ProductionPlanTimeSeriesApply,
            class_list=ProductionPlanTimeSeriesList,
        )
        self.view_id = view_id

    def apply(
        self,
        production_plan_time_series: ProductionPlanTimeSeriesApply | Sequence[ProductionPlanTimeSeriesApply],
        replace: bool = False,
    ) -> dm.InstancesApplyResult:
        if isinstance(production_plan_time_series, ProductionPlanTimeSeriesApply):
            instances = production_plan_time_series.to_instances_apply()
        else:
            instances = ProductionPlanTimeSeriesApplyList(production_plan_time_series).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(ProductionPlanTimeSeriesApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(ProductionPlanTimeSeriesApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> ProductionPlanTimeSeries:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ProductionPlanTimeSeriesList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> ProductionPlanTimeSeries | ProductionPlanTimeSeriesList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ProductionPlanTimeSeriesList:
        filter_ = _create_filter(
            self.view_id,
            name,
            name_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
