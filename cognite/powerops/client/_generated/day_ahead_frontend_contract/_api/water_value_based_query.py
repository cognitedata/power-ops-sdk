from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes import (
    WaterValueBased,
    WaterValueBasedApply,
)
from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes._water_value_based import (
    _WATERVALUEBASED_PROPERTIES_BY_FIELD,
)


class WaterValueBasedQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_water_value_based: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_water_value_based: Whether to retrieve the water value based or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_water_value_based and not self._builder[-1].name.startswith("water_value_based"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("water_value_based"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[WaterValueBasedApply],
                                list(_WATERVALUEBASED_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=WaterValueBased,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
