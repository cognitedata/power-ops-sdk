from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    ShopBasedPartialBidConfiguration,
    PowerAsset,
    ShopScenarioSet,
)
from cognite.powerops.client._generated.v1._api._core import (
    DEFAULT_QUERY_LIMIT,
    EdgeQueryStep,
    NodeQueryStep,
    DataClassQueryBuilder,
    QueryAPI,
    T_DomainModelList,
    _create_edge_filter,
)



class ShopBasedPartialBidConfigurationQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("power_ops_core", "ShopBasedPartialBidConfiguration", "1")

    def __init__(
        self,
        client: CogniteClient,
        builder: DataClassQueryBuilder[T_DomainModelList],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder)
        from_ = self._builder.get_from()
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    filter=filter_,
                ),
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
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("powerAsset"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[PowerAsset._view_id]),
                ),
                result_cls=PowerAsset,
            ),
        )
    def _query_append_scenario_set(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("scenarioSet"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[ShopScenarioSet._view_id]),
                ),
                result_cls=ShopScenarioSet,
            ),
        )
