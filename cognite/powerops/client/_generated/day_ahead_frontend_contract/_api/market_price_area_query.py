from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes import (
    MarketPriceArea,
    MarketPriceAreaApply,
)
from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes._market_price_area import (
    _MARKETPRICEAREA_PROPERTIES_BY_FIELD,
)


class MarketPriceAreaQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_market_price_area: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_market_price_area: Whether to retrieve the market price area or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_market_price_area and not self._builder[-1].name.startswith("market_price_area"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("market_price_area"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[MarketPriceAreaApply],
                                list(_MARKETPRICEAREA_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=MarketPriceArea,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
