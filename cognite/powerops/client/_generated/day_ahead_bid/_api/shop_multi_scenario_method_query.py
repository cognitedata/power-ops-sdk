from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.day_ahead_bid.data_classes import (
    DomainModelCore,
    SHOPMultiScenarioMethod,
)
from cognite.powerops.client._generated.day_ahead_bid.data_classes._shop_price_scenario import (
    SHOPPriceScenario,
    _create_shop_price_scenario_filter,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .shop_price_scenario_query import SHOPPriceScenarioQueryAPI


class SHOPMultiScenarioMethodQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("shop_multi_scenario_method"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(self._view_by_read_class[SHOPMultiScenarioMethod], ["*"])]
                ),
                result_cls=SHOPMultiScenarioMethod,
                max_retrieve_limit=limit,
            )
        )

    def price_scenarios(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
    ) -> SHOPPriceScenarioQueryAPI[T_DomainModelList]:
        """Query along the price scenario edges of the shop multi scenario method.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of price scenario edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.

        Returns:
            SHOPPriceScenarioQueryAPI: The query API for the shop price scenario.
        """
        from .shop_price_scenario_query import SHOPPriceScenarioQueryAPI

        from_ = self._builder[-1].name
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power-ops-types", "PriceScenario"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("price_scenarios"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )

        view_id = self._view_by_read_class[SHOPPriceScenario]
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_shop_price_scenario_filter(
            view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        return SHOPPriceScenarioQueryAPI(self._client, self._builder, self._view_by_read_class, node_filer, limit)

    def query(
        self,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Returns:
            The list of the source nodes of the query.

        """
        return self._query()
