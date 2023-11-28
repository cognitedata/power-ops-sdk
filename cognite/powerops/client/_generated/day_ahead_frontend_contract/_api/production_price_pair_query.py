from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes import (
    ProductionPricePair,
    ProductionPricePairApply,
)
from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes._production_price_pair import (
    _PRODUCTIONPRICEPAIR_PROPERTIES_BY_FIELD,
)


class ProductionPricePairQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_production_price_pair: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_production_price_pair: Whether to retrieve the production price pair or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_production_price_pair and not self._builder[-1].name.startswith("production_price_pair"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("production_price_pair"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[ProductionPricePairApply],
                                list(_PRODUCTIONPRICEPAIR_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=ProductionPricePair,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
