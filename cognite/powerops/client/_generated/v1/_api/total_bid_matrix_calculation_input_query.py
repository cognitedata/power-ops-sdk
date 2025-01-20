from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    TotalBidMatrixCalculationInput,
    BidConfigurationDayAhead,
)
from cognite.powerops.client._generated.v1.data_classes._bid_matrix import (
    BidMatrix,
    _create_bid_matrix_filter,
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
    from cognite.powerops.client._generated.v1._api.bid_matrix_query import BidMatrixQueryAPI


class TotalBidMatrixCalculationInputQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("power_ops_core", "TotalBidMatrixCalculationInput", "1")

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
                result_cls=TotalBidMatrixCalculationInput,
                max_retrieve_limit=limit,
            )
        )
    def partial_bid_matrices(
        self,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        retrieve_bid_configuration: bool = False,
    ) -> BidMatrixQueryAPI[T_DomainModelList]:
        """Query along the partial bid matrice edges of the total bid matrix calculation input.

        Args:
            state: The state to filter on.
            state_prefix: The prefix of the state to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of partial bid matrice edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_bid_configuration: Whether to retrieve the bid configuration for each total bid matrix calculation input or not.

        Returns:
            BidMatrixQueryAPI: The query API for the bid matrix.
        """
        from .bid_matrix_query import BidMatrixQueryAPI

        # from is a string as we added a node query step in the __init__ method
        from_ = cast(str, self._builder.get_from())
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "BidMatrix"),
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

        view_id = BidMatrixQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_bid_matrix_filter(
            view_id,
            state,
            state_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_bid_configuration:
            self._query_append_bid_configuration(from_)
        return BidMatrixQueryAPI(self._client, self._builder, node_filer, limit)

    def query(
        self,
        retrieve_bid_configuration: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_bid_configuration: Whether to retrieve the bid configuration for each total bid matrix calculation input or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_bid_configuration:
            self._query_append_bid_configuration(from_)
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
