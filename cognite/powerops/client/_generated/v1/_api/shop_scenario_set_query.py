from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    ShopScenarioSet,
    DateSpecification,
    DateSpecification,
)
from cognite.powerops.client._generated.v1.data_classes._shop_scenario import (
    ShopScenario,
    _create_shop_scenario_filter,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .shop_scenario_query import ShopScenarioQueryAPI



class ShopScenarioSetQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("power_ops_core", "ShopScenarioSet", "1")

    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder)

        self._builder.append(
            QueryStep(
                name=self._builder.next_name("shop_scenario_set"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_id, ["*"])]),
                result_cls=ShopScenarioSet,
                max_retrieve_limit=limit,
            )
        )

    def scenarios(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        retrieve_start_specification: bool = False,
        retrieve_end_specification: bool = False,
    ) -> ShopScenarioQueryAPI[T_DomainModelList]:
        """Query along the scenario edges of the shop scenario set.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            model: The model to filter on.
            commands: The command to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of scenario edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_start_specification: Whether to retrieve the start specification for each shop scenario set or not.
            retrieve_end_specification: Whether to retrieve the end specification for each shop scenario set or not.

        Returns:
            ShopScenarioQueryAPI: The query API for the shop scenario.
        """
        from .shop_scenario_query import ShopScenarioQueryAPI

        from_ = self._builder[-1].name
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "ShopScenarioSet.scenarios"),

            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("scenarios"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )

        view_id = ShopScenarioQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_shop_scenario_filter(
            view_id,
            name,
            name_prefix,
            model,
            commands,
            source,
            source_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_start_specification:
            self._query_append_start_specification(from_)
        if retrieve_end_specification:
            self._query_append_end_specification(from_)
        return ShopScenarioQueryAPI(self._client, self._builder, node_filer, limit)

    def query(
        self,
        retrieve_start_specification: bool = False,
        retrieve_end_specification: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_start_specification: Whether to retrieve the start specification for each shop scenario set or not.
            retrieve_end_specification: Whether to retrieve the end specification for each shop scenario set or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_start_specification:
            self._query_append_start_specification(from_)
        if retrieve_end_specification:
            self._query_append_end_specification(from_)
        return self._query()

    def _query_append_start_specification(self, from_: str) -> None:
        view_id = DateSpecification._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("start_specification"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("startSpecification"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=DateSpecification,
                is_single_direct_relation=True,
            ),
        )

    def _query_append_end_specification(self, from_: str) -> None:
        view_id = DateSpecification._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("end_specification"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("endSpecification"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=DateSpecification,
                is_single_direct_relation=True,
            ),
        )
