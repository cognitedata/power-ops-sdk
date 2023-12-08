from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from cognite.powerops.client._generated.day_ahead_bids.data_classes import (
    SHOPMultiScenario,
    SHOPMultiScenarioApply,
)
from cognite.powerops.client._generated.day_ahead_bids.data_classes._shop_multi_scenario import (
    _SHOPMULTISCENARIO_PROPERTIES_BY_FIELD,
)


class SHOPMultiScenarioQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_shop_multi_scenario: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_shop_multi_scenario: Whether to retrieve the shop multi scenario or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_shop_multi_scenario and not self._builder[-1].name.startswith("shop_multi_scenario"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("shop_multi_scenario"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[SHOPMultiScenarioApply],
                                list(_SHOPMULTISCENARIO_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=SHOPMultiScenario,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
