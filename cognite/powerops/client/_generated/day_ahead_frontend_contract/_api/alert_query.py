from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes import (
    Alert,
    AlertApply,
)
from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes._alert import (
    _ALERT_PROPERTIES_BY_FIELD,
)


class AlertQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_alert: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_alert: Whether to retrieve the alert or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_alert and not self._builder[-1].name.startswith("alert"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("alert"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[AlertApply],
                                list(_ALERT_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=Alert,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
