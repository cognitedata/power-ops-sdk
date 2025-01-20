from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    BenchmarkingShopCase,
    ShopScenario,
)
from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    ViewPropertyId,
    T_DomainModel,
    T_DomainModelList,
    QueryBuilder,
    QueryStep,
)
from cognite.powerops.client._generated.v1.data_classes._shop_file import (
    _create_shop_file_filter,
)
from cognite.powerops.client._generated.v1._api._core import (
    QueryAPI,
    _create_edge_filter,
)

if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1._api.shop_file_query import ShopFileQueryAPI


class BenchmarkingShopCaseQueryAPI(QueryAPI[T_DomainModel, T_DomainModelList]):
    _view_id = dm.ViewId("power_ops_core", "BenchmarkingShopCase", "1")

    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder,
        result_cls: type[T_DomainModel],
        result_list_cls: type[T_DomainModelList],
        connection_property: ViewPropertyId | None = None,
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder, result_cls, result_list_cls)
        from_ = self._builder.get_from()
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    filter=filter_,
                ),
                max_retrieve_limit=limit,
                view_id=self._view_id,
                connection_property=connection_property,
            )
        )
    def shop_files(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        label: str | list[str] | None = None,
        label_prefix: str | None = None,
        file_reference_prefix: str | list[str] | None = None,
        file_reference_prefix_prefix: str | None = None,
        min_order: int | None = None,
        max_order: int | None = None,
        is_ascii: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        retrieve_scenario: bool = False,
    ) -> ShopFileQueryAPI[T_DomainModel, T_DomainModelList]:
        """Query along the shop file edges of the benchmarking shop case.

        Args:
            name:
            name_prefix:
            label:
            label_prefix:
            file_reference_prefix:
            file_reference_prefix_prefix:
            min_order:
            max_order:
            is_ascii:
            external_id_prefix:
            space:
            external_id_prefix_edge:
            space_edge:
            filter: (Advanced) Filter applied to node. If the filtering available in the
                above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of shop file edges to return.
                Defaults to 3. Set to -1, float("inf") or None to return all items.
            retrieve_scenario: Whether to retrieve the scenario
                for each benchmarking shop case or not.

        Returns:
            ShopFileQueryAPI: The query API for the shop file.
        """
        from .shop_file_query import ShopFileQueryAPI

        # from is a string as we added a node query step in the __init__ method
        from_ = cast(str, self._builder.get_from())
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "ShopCase.shopFiles"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                max_retrieve_limit=limit,
                connection_property=ViewPropertyId(self._view_id, "shopFiles"),
            )
        )

        view_id = ShopFileQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filter = _create_shop_file_filter(
            view_id,
            name,
            name_prefix,
            label,
            label_prefix,
            file_reference_prefix,
            file_reference_prefix_prefix,
            min_order,
            max_order,
            is_ascii,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_scenario:
            self._query_append_scenario(from_)
        return (ShopFileQueryAPI(
            self._client,
            self._builder,
            self._result_cls,
            self._result_list_cls,
            ViewPropertyId(self._view_id, "end_node"),
            node_filter,
            limit,
        ))

    def query(
        self,
        retrieve_scenario: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_scenario: Whether to retrieve the
                scenario for each
                benchmarking shop case or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_scenario:
            self._query_append_scenario(from_)
        return self._query()

    def _query_append_scenario(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("scenario"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[ShopScenario._view_id]),
                ),
                view_id=ShopScenario._view_id,
                connection_property=ViewPropertyId(self._view_id, "scenario"),
            ),
        )
