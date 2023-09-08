from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated._api._core import TypeAPI
from cognite.powerops.client._generated.data_classes import (
    ProductionPlanTimeSeries,
    ProductionPlanTimeSeriesApply,
    ProductionPlanTimeSeriesList,
)


class ProductionPlanTimeSeriesAPI(
    TypeAPI[ProductionPlanTimeSeries, ProductionPlanTimeSeriesApply, ProductionPlanTimeSeriesList]
):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "ProductionPlanTimeSeries", "ca7ffcb6f63d3f"),
            class_type=ProductionPlanTimeSeries,
            class_apply_type=ProductionPlanTimeSeriesApply,
            class_list=ProductionPlanTimeSeriesList,
        )

    def apply(
        self, production_plan_time_series: ProductionPlanTimeSeriesApply, replace: bool = False
    ) -> dm.InstancesApplyResult:
        instances = production_plan_time_series.to_instances_apply()
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

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> ProductionPlanTimeSeriesList:
        return self._list(limit=limit)
