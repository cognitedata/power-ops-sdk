from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    WaterPartialBidCalculationInput,
    BidCalculationTask,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter


class WaterPartialBidCalculationInputQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("water_partial_bid_calculation_input"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select(
                    [dm.query.SourceSelector(self._view_by_read_class[WaterPartialBidCalculationInput], ["*"])]
                ),
                result_cls=WaterPartialBidCalculationInput,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_calculation_task: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_calculation_task: Whether to retrieve the calculation task for each water partial bid calculation input or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_calculation_task:
            self._query_append_calculation_task(from_)
        return self._query()

    def _query_append_calculation_task(self, from_: str) -> None:
        view_id = self._view_by_read_class[BidCalculationTask]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("calculation_task"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[WaterPartialBidCalculationInput].as_property_ref(
                        "calculationTask"
                    ),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=BidCalculationTask,
            ),
        )
