from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import Point, PointApply, PointApplyList, PointList

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class PointAPI(TypeAPI[Point, PointApply, PointList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Point,
            class_apply_type=PointApply,
            class_list=PointList,
        )
        self.view_id = view_id

    def apply(self, point: PointApply | Sequence[PointApply], replace: bool = False) -> dm.InstancesApplyResult:
        if isinstance(point, PointApply):
            instances = point.to_instances_apply()
        else:
            instances = PointApplyList(point).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(PointApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(PointApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Point:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> PointList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Point | PointList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(
        self,
        min_position: int | None = None,
        max_position: int | None = None,
        min_quantity: float | None = None,
        max_quantity: float | None = None,
        min_minimum_quantity: float | None = None,
        max_minimum_quantity: float | None = None,
        min_price_amount: float | None = None,
        max_price_amount: float | None = None,
        min_energy_price_amount: float | None = None,
        max_energy_price_amount: float | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PointList:
        filter_ = _create_filter(
            self.view_id,
            min_position,
            max_position,
            min_quantity,
            max_quantity,
            min_minimum_quantity,
            max_minimum_quantity,
            min_price_amount,
            max_price_amount,
            min_energy_price_amount,
            max_energy_price_amount,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    min_position: int | None = None,
    max_position: int | None = None,
    min_quantity: float | None = None,
    max_quantity: float | None = None,
    min_minimum_quantity: float | None = None,
    max_minimum_quantity: float | None = None,
    min_price_amount: float | None = None,
    max_price_amount: float | None = None,
    min_energy_price_amount: float | None = None,
    max_energy_price_amount: float | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_position or max_position:
        filters.append(dm.filters.Range(view_id.as_property_ref("position"), gte=min_position, lte=max_position))
    if min_quantity or max_quantity:
        filters.append(dm.filters.Range(view_id.as_property_ref("quantity"), gte=min_quantity, lte=max_quantity))
    if min_minimum_quantity or max_minimum_quantity:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("minimumQuantity"), gte=min_minimum_quantity, lte=max_minimum_quantity
            )
        )
    if min_price_amount or max_price_amount:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("priceAmount"), gte=min_price_amount, lte=max_price_amount)
        )
    if min_energy_price_amount or max_energy_price_amount:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("energyPriceAmount"), gte=min_energy_price_amount, lte=max_energy_price_amount
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
