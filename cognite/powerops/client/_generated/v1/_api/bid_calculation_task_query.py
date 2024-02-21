from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    BidCalculationTask,
    Plant,
    PriceArea,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter


class BidCalculationTaskQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("bid_calculation_task"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_read_class[BidCalculationTask], ["*"])]),
                result_cls=BidCalculationTask,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_plant: bool = False,
        retrieve_price_area: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_plant: Whether to retrieve the plant for each bid calculation task or not.
            retrieve_price_area: Whether to retrieve the price area for each bid calculation task or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_plant:
            self._query_append_plant(from_)
        if retrieve_price_area:
            self._query_append_price_area(from_)
        return self._query()

    def _query_append_plant(self, from_: str) -> None:
        view_id = self._view_by_read_class[Plant]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("plant"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[BidCalculationTask].as_property_ref("plant"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=Plant,
            ),
        )

    def _query_append_price_area(self, from_: str) -> None:
        view_id = self._view_by_read_class[PriceArea]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("price_area"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[BidCalculationTask].as_property_ref("priceArea"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=PriceArea,
            ),
        )
