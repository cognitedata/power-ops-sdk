from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    ShopCase,
    ShopScenario,
)
from cognite.powerops.client._generated.v1.data_classes._shop_file import (
    ShopFile,
    _create_shop_file_filter,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .shop_file_query import ShopFileQueryAPI


class ShopCaseQueryAPI(QueryAPI[T_DomainModelList]):
    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder, view_by_read_class)

        self._builder.append(
            QueryStep(
                name=self._builder.next_name("shop_case"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_read_class[ShopCase], ["*"])]),
                result_cls=ShopCase,
                max_retrieve_limit=limit,
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
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_scenario: bool = False,
    ) -> ShopFileQueryAPI[T_DomainModelList]:
        """Query along the shop file edges of the shop case.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            label: The label to filter on.
            label_prefix: The prefix of the label to filter on.
            file_reference_prefix: The file reference prefix to filter on.
            file_reference_prefix_prefix: The prefix of the file reference prefix to filter on.
            min_order: The minimum value of the order to filter on.
            max_order: The maximum value of the order to filter on.
            is_ascii: The is ascii to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of shop file edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_scenario: Whether to retrieve the scenario for each shop case or not.

        Returns:
            ShopFileQueryAPI: The query API for the shop file.
        """
        from .shop_file_query import ShopFileQueryAPI

        from_ = self._builder[-1].name
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "ShopCase.shopFiles"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("shop_files"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )

        view_id = self._view_by_read_class[ShopFile]
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_shop_file_filter(
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
        return ShopFileQueryAPI(self._client, self._builder, self._view_by_read_class, node_filer, limit)

    def query(
        self,
        retrieve_scenario: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_scenario: Whether to retrieve the scenario for each shop case or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_scenario:
            self._query_append_scenario(from_)
        return self._query()

    def _query_append_scenario(self, from_: str) -> None:
        view_id = self._view_by_read_class[ShopScenario]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("scenario"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[ShopCase].as_property_ref("scenario"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=ShopScenario,
                is_single_direct_relation=True,
            ),
        )
