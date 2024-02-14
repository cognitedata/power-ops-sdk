from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    TaskDispatcherShopInput,
    BidConfigurationShop,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter


class TaskDispatcherShopInputQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("task_dispatcher_shop_input"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(self._view_by_read_class[TaskDispatcherShopInput], ["*"])]
                ),
                result_cls=TaskDispatcherShopInput,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_bid_configuration: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_bid_configuration: Whether to retrieve the bid configuration for each task dispatcher shop input or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_bid_configuration:
            self._query_append_bid_configuration(from_)
        return self._query()

    def _query_append_bid_configuration(self, from_: str) -> None:
        view_id = self._view_by_read_class[BidConfigurationShop]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("bid_configuration"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[TaskDispatcherShopInput].as_property_ref("bidConfiguration"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=BidConfigurationShop,
            ),
        )
