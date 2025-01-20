from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    MultiScenarioPartialBidMatrixCalculationInput,
    BidConfigurationDayAhead,
    ShopBasedPartialBidConfiguration,
)
from cognite.powerops.client._generated.v1.data_classes._price_production import (
    PriceProduction,
    _create_price_production_filter,
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

if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1._api.price_production_query import PriceProductionQueryAPI


class MultiScenarioPartialBidMatrixCalculationInputQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("power_ops_core", "MultiScenarioPartialBidMatrixCalculationInput", "1")

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
                result_cls=MultiScenarioPartialBidMatrixCalculationInput,
                max_retrieve_limit=limit,
            )
        )
    def price_production(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        shop_result: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        retrieve_bid_configuration: bool = False,
        retrieve_partial_bid_configuration: bool = False,
    ) -> PriceProductionQueryAPI[T_DomainModelList]:
        """Query along the price production edges of the multi scenario partial bid matrix calculation input.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            shop_result: The shop result to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of price production edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_bid_configuration: Whether to retrieve the bid configuration for each multi scenario partial bid matrix calculation input or not.
            retrieve_partial_bid_configuration: Whether to retrieve the partial bid configuration for each multi scenario partial bid matrix calculation input or not.

        Returns:
            PriceProductionQueryAPI: The query API for the price production.
        """
        from .price_production_query import PriceProductionQueryAPI

        # from is a string as we added a node query step in the __init__ method
        from_ = cast(str, self._builder.get_from())
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "PriceProduction"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            EdgeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                max_retrieve_limit=limit,
            )
        )

        view_id = PriceProductionQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_price_production_filter(
            view_id,
            name,
            name_prefix,
            shop_result,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_bid_configuration:
            self._query_append_bid_configuration(from_)
        if retrieve_partial_bid_configuration:
            self._query_append_partial_bid_configuration(from_)
        return PriceProductionQueryAPI(self._client, self._builder, node_filer, limit)

    def query(
        self,
        retrieve_bid_configuration: bool = False,
        retrieve_partial_bid_configuration: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_bid_configuration: Whether to retrieve the bid configuration for each multi scenario partial bid matrix calculation input or not.
            retrieve_partial_bid_configuration: Whether to retrieve the partial bid configuration for each multi scenario partial bid matrix calculation input or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_bid_configuration:
            self._query_append_bid_configuration(from_)
        if retrieve_partial_bid_configuration:
            self._query_append_partial_bid_configuration(from_)
        return self._query()

    def _query_append_bid_configuration(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("bidConfiguration"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[BidConfigurationDayAhead._view_id]),
                ),
                result_cls=BidConfigurationDayAhead,
            ),
        )
    def _query_append_partial_bid_configuration(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("partialBidConfiguration"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[ShopBasedPartialBidConfiguration._view_id]),
                ),
                result_cls=ShopBasedPartialBidConfiguration,
            ),
        )
