from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    TaskDispatcherShopOutput,
    TaskDispatcherShopInput,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .alert_query import AlertQueryAPI
    from .shop_partial_bid_calculation_input_query import ShopPartialBidCalculationInputQueryAPI
    from .preprocessor_input_query import PreprocessorInputQueryAPI


class TaskDispatcherShopOutputQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("task_dispatcher_shop_output"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(self._view_by_read_class[TaskDispatcherShopOutput], ["*"])]
                ),
                result_cls=TaskDispatcherShopOutput,
                max_retrieve_limit=limit,
            )
        )

    def alerts(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_input_: bool = False,
    ) -> AlertQueryAPI[T_DomainModelList]:
        """Query along the alert edges of the task dispatcher shop output.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of alert edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_input_: Whether to retrieve the input for each task dispatcher shop output or not.

        Returns:
            AlertQueryAPI: The query API for the alert.
        """
        from .alert_query import AlertQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "calculationIssue"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("alerts"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_input_:
            self._query_append_input_(from_)
        return AlertQueryAPI(self._client, self._builder, self._view_by_read_class, None, limit)

    def partial_bid_calculations(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_input_: bool = False,
    ) -> ShopPartialBidCalculationInputQueryAPI[T_DomainModelList]:
        """Query along the partial bid calculation edges of the task dispatcher shop output.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial bid calculation edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_input_: Whether to retrieve the input for each task dispatcher shop output or not.

        Returns:
            ShopPartialBidCalculationInputQueryAPI: The query API for the shop partial bid calculation input.
        """
        from .shop_partial_bid_calculation_input_query import ShopPartialBidCalculationInputQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "ShopPartialBidCalculationInput"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("partial_bid_calculations"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_input_:
            self._query_append_input_(from_)
        return ShopPartialBidCalculationInputQueryAPI(
            self._client, self._builder, self._view_by_read_class, None, limit
        )

    def preprocessor_calculations(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_input_: bool = False,
    ) -> PreprocessorInputQueryAPI[T_DomainModelList]:
        """Query along the preprocessor calculation edges of the task dispatcher shop output.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of preprocessor calculation edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_input_: Whether to retrieve the input for each task dispatcher shop output or not.

        Returns:
            PreprocessorInputQueryAPI: The query API for the preprocessor input.
        """
        from .preprocessor_input_query import PreprocessorInputQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "PreprocessorInput"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("preprocessor_calculations"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_input_:
            self._query_append_input_(from_)
        return PreprocessorInputQueryAPI(self._client, self._builder, self._view_by_read_class, None, limit)

    def query(
        self,
        retrieve_input_: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_input_: Whether to retrieve the input for each task dispatcher shop output or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_input_:
            self._query_append_input_(from_)
        return self._query()

    def _query_append_input_(self, from_: str) -> None:
        view_id = self._view_by_read_class[TaskDispatcherShopInput]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("input_"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[TaskDispatcherShopOutput].as_property_ref("input"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=TaskDispatcherShopInput,
            ),
        )
