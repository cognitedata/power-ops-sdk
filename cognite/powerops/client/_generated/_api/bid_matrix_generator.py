from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import (
    BidMatrixGenerator,
    BidMatrixGeneratorApply,
    BidMatrixGeneratorApplyList,
    BidMatrixGeneratorList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class BidMatrixGeneratorAPI(TypeAPI[BidMatrixGenerator, BidMatrixGeneratorApply, BidMatrixGeneratorList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BidMatrixGenerator,
            class_apply_type=BidMatrixGeneratorApply,
            class_list=BidMatrixGeneratorList,
        )
        self.view_id = view_id

    def apply(
        self, bid_matrix_generator: BidMatrixGeneratorApply | Sequence[BidMatrixGeneratorApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(bid_matrix_generator, BidMatrixGeneratorApply):
            instances = bid_matrix_generator.to_instances_apply()
        else:
            instances = BidMatrixGeneratorApplyList(bid_matrix_generator).to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(BidMatrixGeneratorApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(BidMatrixGeneratorApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> BidMatrixGenerator:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> BidMatrixGeneratorList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> BidMatrixGenerator | BidMatrixGeneratorList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(
        self,
        shop_plant: str | list[str] | None = None,
        shop_plant_prefix: str | None = None,
        methods: str | list[str] | None = None,
        methods_prefix: str | None = None,
        function_external_id: str | list[str] | None = None,
        function_external_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidMatrixGeneratorList:
        filter_ = _create_filter(
            self.view_id,
            shop_plant,
            shop_plant_prefix,
            methods,
            methods_prefix,
            function_external_id,
            function_external_id_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    shop_plant: str | list[str] | None = None,
    shop_plant_prefix: str | None = None,
    methods: str | list[str] | None = None,
    methods_prefix: str | None = None,
    function_external_id: str | list[str] | None = None,
    function_external_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if shop_plant and isinstance(shop_plant, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopPlant"), value=shop_plant))
    if shop_plant and isinstance(shop_plant, list):
        filters.append(dm.filters.In(view_id.as_property_ref("shopPlant"), values=shop_plant))
    if shop_plant_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("shopPlant"), value=shop_plant_prefix))
    if methods and isinstance(methods, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("methods"), value=methods))
    if methods and isinstance(methods, list):
        filters.append(dm.filters.In(view_id.as_property_ref("methods"), values=methods))
    if methods_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("methods"), value=methods_prefix))
    if function_external_id and isinstance(function_external_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("functionExternalId"), value=function_external_id))
    if function_external_id and isinstance(function_external_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("functionExternalId"), values=function_external_id))
    if function_external_id_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("functionExternalId"), value=function_external_id_prefix)
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
