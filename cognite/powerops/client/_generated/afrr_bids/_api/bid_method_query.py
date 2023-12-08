from __future__ import annotations

from typing import TYPE_CHECKING
from cognite.client import data_modeling as dm
from ._core import DEFAULT_QUERY_LIMIT, QueryStep, QueryAPI, T_DomainModelList
from cognite.powerops.client._generated.afrr_bids.data_classes import (
    BidMethod,
    BidMethodApply,
)
from cognite.powerops.client._generated.afrr_bids.data_classes._bid_method import (
    _BIDMETHOD_PROPERTIES_BY_FIELD,
)


class BidMethodQueryAPI(QueryAPI[T_DomainModelList]):
    def query(
        self,
        retrieve_bid_method: bool = True,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_bid_method: Whether to retrieve the bid method or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_bid_method and not self._builder[-1].name.startswith("bid_method"):
            self._builder.append(
                QueryStep(
                    name=self._builder.next_name("bid_method"),
                    expression=dm.query.NodeResultSetExpression(
                        filter=None,
                        from_=from_,
                    ),
                    select=dm.query.Select(
                        [
                            dm.query.SourceSelector(
                                self._view_by_write_class[BidMethodApply],
                                list(_BIDMETHOD_PROPERTIES_BY_FIELD.values()),
                            )
                        ]
                    ),
                    result_cls=BidMethod,
                    max_retrieve_limit=-1,
                ),
            )

        return self._query()
