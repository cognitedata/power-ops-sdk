from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from cognite.powerops.client._generated.day_ahead_bids.data_classes import (
    PriceArea,
    PriceAreaApply,
)
from cognite.powerops.client._generated.day_ahead_bids.data_classes._price_area import (
    _PRICEAREA_PROPERTIES_BY_FIELD,
)


class PriceAreaQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_price_area: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_price_area: Whether to retrieve the price area or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_price_area and not self._builder[-1].name.startswith("price_area"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("price_area"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[PriceAreaApply],
                                list(_PRICEAREA_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=PriceArea,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
