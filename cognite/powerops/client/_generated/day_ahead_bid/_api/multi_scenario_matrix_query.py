from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.day_ahead_bid.data_classes import (
    DomainModelCore,
    MultiScenarioMatrix,
    BidMethod,
)
from cognite.powerops.client._generated.day_ahead_bid.data_classes._alert import (
    Alert,
    _create_alert_filter,
)
from cognite.powerops.client._generated.day_ahead_bid.data_classes._shop_price_scenario_result import (
    SHOPPriceScenarioResult,
    _create_shop_price_scenario_result_filter,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .alert_query import AlertQueryAPI
    from .shop_price_scenario_result_query import SHOPPriceScenarioResultQueryAPI


class MultiScenarioMatrixQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("multi_scenario_matrix"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_read_class[MultiScenarioMatrix], ["*"])]),
                result_cls=MultiScenarioMatrix,
                max_retrieve_limit=limit,
            )
        )

    def alerts(
        self,
        min_time: datetime.datetime | None = None,
        max_time: datetime.datetime | None = None,
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
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_method: bool = False,
    ) -> AlertQueryAPI[T_DomainModelList]:
        """Query along the alert edges of the multi scenario matrix.

        Args:
            min_time: The minimum value of the time to filter on.
            max_time: The maximum value of the time to filter on.
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
            retrieve_method: Whether to retrieve the method for each multi scenario matrix or not.

        Returns:
            AlertQueryAPI: The query API for the alert.
        """
        from .alert_query import AlertQueryAPI

        from_ = self._builder[-1].name
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power-ops-types", "calculationIssue"),
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

        view_id = self._view_by_read_class[Alert]
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_alert_filter(
            view_id,
            min_time,
            max_time,
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
        if retrieve_method:
            self._query_append_method(from_)
        return AlertQueryAPI(self._client, self._builder, self._view_by_read_class, node_filer, limit)

    def scenario_results(
        self,
        price_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_method: bool = False,
    ) -> SHOPPriceScenarioResultQueryAPI[T_DomainModelList]:
        """Query along the scenario result edges of the multi scenario matrix.

        Args:
            price_scenario: The price scenario to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of scenario result edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_method: Whether to retrieve the method for each multi scenario matrix or not.

        Returns:
            SHOPPriceScenarioResultQueryAPI: The query API for the shop price scenario result.
        """
        from .shop_price_scenario_result_query import SHOPPriceScenarioResultQueryAPI

        from_ = self._builder[-1].name
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power-ops-types", "scenarioResult"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("scenario_results"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )

        view_id = self._view_by_read_class[SHOPPriceScenarioResult]
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_shop_price_scenario_result_filter(
            view_id,
            price_scenario,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_method:
            self._query_append_method(from_)
        return SHOPPriceScenarioResultQueryAPI(self._client, self._builder, self._view_by_read_class, node_filer, limit)

    def query(
        self,
        retrieve_method: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_method: Whether to retrieve the method for each multi scenario matrix or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_method:
            self._query_append_method(from_)
        return self._query()

    def _query_append_method(self, from_: str) -> None:
        view_id = self._view_by_read_class[BidMethod]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("method"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[MultiScenarioMatrix].as_property_ref("method"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=BidMethod,
                is_single_direct_relation=True,
            ),
        )
