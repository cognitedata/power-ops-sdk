from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    ShopBasedPartialBidConfiguration,
    PowerAsset,
    ShopScenarioSet,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter



class ShopBasedPartialBidConfigurationQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("power_ops_core", "ShopBasedPartialBidConfiguration", "1")

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
                name=self._builder.next_name("shop_based_partial_bid_configuration"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_id, ["*"])]),
                result_cls=ShopBasedPartialBidConfiguration,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_power_asset: bool = False,
        retrieve_scenario_set: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_power_asset: Whether to retrieve the power asset for each shop based partial bid configuration or not.
            retrieve_scenario_set: Whether to retrieve the scenario set for each shop based partial bid configuration or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_power_asset:
            self._query_append_power_asset(from_)
        if retrieve_scenario_set:
            self._query_append_scenario_set(from_)
        return self._query()

    def _query_append_power_asset(self, from_: str) -> None:
        view_id = PowerAsset._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("power_asset"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("powerAsset"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=PowerAsset,
                is_single_direct_relation=True,
            ),
        )

    def _query_append_scenario_set(self, from_: str) -> None:
        view_id = ShopScenarioSet._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("scenario_set"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("scenarioSet"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=ShopScenarioSet,
                is_single_direct_relation=True,
            ),
        )
