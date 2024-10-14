from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    PartialBidMatrixCalculationOutput,
    PartialBidMatrixCalculationInput,
    BidMatrix,
    BidConfigurationDayAhead,
)
from cognite.powerops.client._generated.v1.data_classes._alert import (
    Alert,
    _create_alert_filter,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .alert_query import AlertQueryAPI



class PartialBidMatrixCalculationOutputQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("power_ops_core", "PartialBidMatrixCalculationOutput", "1")

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
                name=self._builder.next_name("partial_bid_matrix_calculation_output"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_id, ["*"])]),
                result_cls=PartialBidMatrixCalculationOutput,
                max_retrieve_limit=limit,
            )
        )

    def alerts(
        self,
        min_time: datetime.datetime | None = None,
        max_time: datetime.datetime | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        severity: str | list[str] | None = None,
        severity_prefix: str | None = None,
        alert_type: str | list[str] | None = None,
        alert_type_prefix: str | None = None,
        min_status_code: int | None = None,
        max_status_code: int | None = None,
        calculation_run: str | list[str] | None = None,
        calculation_run_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        retrieve_function_input: bool = False,
        retrieve_partial_matrix: bool = False,
        retrieve_bid_configuration: bool = False,
    ) -> AlertQueryAPI[T_DomainModelList]:
        """Query along the alert edges of the partial bid matrix calculation output.

        Args:
            min_time: The minimum value of the time to filter on.
            max_time: The maximum value of the time to filter on.
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            title: The title to filter on.
            title_prefix: The prefix of the title to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            severity: The severity to filter on.
            severity_prefix: The prefix of the severity to filter on.
            alert_type: The alert type to filter on.
            alert_type_prefix: The prefix of the alert type to filter on.
            min_status_code: The minimum value of the status code to filter on.
            max_status_code: The maximum value of the status code to filter on.
            calculation_run: The calculation run to filter on.
            calculation_run_prefix: The prefix of the calculation run to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of alert edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_function_input: Whether to retrieve the function input for each partial bid matrix calculation output or not.
            retrieve_partial_matrix: Whether to retrieve the partial matrix for each partial bid matrix calculation output or not.
            retrieve_bid_configuration: Whether to retrieve the bid configuration for each partial bid matrix calculation output or not.

        Returns:
            AlertQueryAPI: The query API for the alert.
        """
        from .alert_query import AlertQueryAPI

        from_ = self._builder[-1].name
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "calculationIssue"),

            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("alerts"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )

        view_id = AlertQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_alert_filter(
            view_id,
            min_time,
            max_time,
            workflow_execution_id,
            workflow_execution_id_prefix,
            title,
            title_prefix,
            description,
            description_prefix,
            severity,
            severity_prefix,
            alert_type,
            alert_type_prefix,
            min_status_code,
            max_status_code,
            calculation_run,
            calculation_run_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_function_input:
            self._query_append_function_input(from_)
        if retrieve_partial_matrix:
            self._query_append_partial_matrix(from_)
        if retrieve_bid_configuration:
            self._query_append_bid_configuration(from_)
        return AlertQueryAPI(self._client, self._builder, node_filer, limit)

    def query(
        self,
        retrieve_function_input: bool = False,
        retrieve_partial_matrix: bool = False,
        retrieve_bid_configuration: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_function_input: Whether to retrieve the function input for each partial bid matrix calculation output or not.
            retrieve_partial_matrix: Whether to retrieve the partial matrix for each partial bid matrix calculation output or not.
            retrieve_bid_configuration: Whether to retrieve the bid configuration for each partial bid matrix calculation output or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_function_input:
            self._query_append_function_input(from_)
        if retrieve_partial_matrix:
            self._query_append_partial_matrix(from_)
        if retrieve_bid_configuration:
            self._query_append_bid_configuration(from_)
        return self._query()

    def _query_append_function_input(self, from_: str) -> None:
        view_id = PartialBidMatrixCalculationInput._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("function_input"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("functionInput"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=PartialBidMatrixCalculationInput,
                is_single_direct_relation=True,
            ),
        )

    def _query_append_partial_matrix(self, from_: str) -> None:
        view_id = BidMatrix._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("partial_matrix"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("partialMatrix"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=BidMatrix,
                is_single_direct_relation=True,
            ),
        )

    def _query_append_bid_configuration(self, from_: str) -> None:
        view_id = BidConfigurationDayAhead._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("bid_configuration"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("bidConfiguration"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=BidConfigurationDayAhead,
                is_single_direct_relation=True,
            ),
        )
