from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    BenchmarkingTaskDispatcherInputDayAhead,
    BenchmarkingConfigurationDayAhead,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter



class BenchmarkingTaskDispatcherInputDayAheadQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("power_ops_core", "BenchmarkingTaskDispatcherInputDayAhead", "1")

    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder)

        self._builder.append(
            QueryStep(
                name=self._builder.next_name("benchmarking_task_dispatcher_input_day_ahead"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_id, ["*"])]),
                result_cls=BenchmarkingTaskDispatcherInputDayAhead,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_benchmarking_config: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_benchmarking_config: Whether to retrieve the benchmarking config for each benchmarking task dispatcher input day ahead or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_benchmarking_config:
            self._query_append_benchmarking_config(from_)
        return self._query()

    def _query_append_benchmarking_config(self, from_: str) -> None:
        view_id = BenchmarkingConfigurationDayAhead._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("benchmarking_config"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("benchmarkingConfig"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=BenchmarkingConfigurationDayAhead,
                is_single_direct_relation=True,
            ),
        )
