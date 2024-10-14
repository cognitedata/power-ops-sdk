from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    PartialBidMatrixInformation,
    PowerAsset,
    PartialBidConfiguration,
)
from cognite.powerops.client._generated.v1.data_classes._alert import (
    Alert,
    _create_alert_filter,
)
from cognite.powerops.client._generated.v1.data_classes._bid_matrix import (
    BidMatrix,
    _create_bid_matrix_filter,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .alert_query import AlertQueryAPI
    from .bid_matrix_query import BidMatrixQueryAPI



class PartialBidMatrixInformationQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("power_ops_core", "PartialBidMatrixInformation", "1")

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
                name=self._builder.next_name("partial_bid_matrix_information"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_id, ["*"])]),
                result_cls=PartialBidMatrixInformation,
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
        retrieve_power_asset: bool = False,
        retrieve_partial_bid_configuration: bool = False,
    ) -> AlertQueryAPI[T_DomainModelList]:
        """Query along the alert edges of the partial bid matrix information.

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
            retrieve_power_asset: Whether to retrieve the power asset for each partial bid matrix information or not.
            retrieve_partial_bid_configuration: Whether to retrieve the partial bid configuration for each partial bid matrix information or not.

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
        if retrieve_power_asset:
            self._query_append_power_asset(from_)
        if retrieve_partial_bid_configuration:
            self._query_append_partial_bid_configuration(from_)
        return AlertQueryAPI(self._client, self._builder, node_filer, limit)

    def underlying_bid_matrices(
        self,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        retrieve_power_asset: bool = False,
        retrieve_partial_bid_configuration: bool = False,
    ) -> BidMatrixQueryAPI[T_DomainModelList]:
        """Query along the underlying bid matrice edges of the partial bid matrix information.

        Args:
            state: The state to filter on.
            state_prefix: The prefix of the state to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of underlying bid matrice edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_power_asset: Whether to retrieve the power asset for each partial bid matrix information or not.
            retrieve_partial_bid_configuration: Whether to retrieve the partial bid configuration for each partial bid matrix information or not.

        Returns:
            BidMatrixQueryAPI: The query API for the bid matrix.
        """
        from .bid_matrix_query import BidMatrixQueryAPI

        from_ = self._builder[-1].name
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "intermediateBidMatrix"),

            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("underlying_bid_matrices"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )

        view_id = BidMatrixQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_bid_matrix_filter(
            view_id,
            state,
            state_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_power_asset:
            self._query_append_power_asset(from_)
        if retrieve_partial_bid_configuration:
            self._query_append_partial_bid_configuration(from_)
        return BidMatrixQueryAPI(self._client, self._builder, node_filer, limit)

    def query(
        self,
        retrieve_power_asset: bool = False,
        retrieve_partial_bid_configuration: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_power_asset: Whether to retrieve the power asset for each partial bid matrix information or not.
            retrieve_partial_bid_configuration: Whether to retrieve the partial bid configuration for each partial bid matrix information or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_power_asset:
            self._query_append_power_asset(from_)
        if retrieve_partial_bid_configuration:
            self._query_append_partial_bid_configuration(from_)
        return self._query()

    def _query_append_power_asset(self, from_: str) -> None:
        view_id = PowerAsset._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("power_asset"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("powerAsset"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=PowerAsset,
                is_single_direct_relation=True,
            ),
        )

    def _query_append_partial_bid_configuration(self, from_: str) -> None:
        view_id = PartialBidConfiguration._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("partial_bid_configuration"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("partialBidConfiguration"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=PartialBidConfiguration,
                is_single_direct_relation=True,
            ),
        )
