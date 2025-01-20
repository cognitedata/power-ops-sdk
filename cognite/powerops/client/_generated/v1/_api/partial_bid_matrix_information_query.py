from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    PartialBidMatrixInformation,
    PowerAsset,
    PartialBidConfiguration,
)
from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    ViewPropertyId,
    T_DomainModel,
    T_DomainModelList,
    QueryBuilder,
    QueryStep,
)
from cognite.powerops.client._generated.v1.data_classes._alert import (
    _create_alert_filter,
)
from cognite.powerops.client._generated.v1.data_classes._bid_matrix import (
    _create_bid_matrix_filter,
)
from cognite.powerops.client._generated.v1._api._core import (
    QueryAPI,
    _create_edge_filter,
)

if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1._api.alert_query import AlertQueryAPI
    from cognite.powerops.client._generated.v1._api.bid_matrix_query import BidMatrixQueryAPI


class PartialBidMatrixInformationQueryAPI(QueryAPI[T_DomainModel, T_DomainModelList]):
    _view_id = dm.ViewId("power_ops_core", "PartialBidMatrixInformation", "1")

    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder,
        result_cls: type[T_DomainModel],
        result_list_cls: type[T_DomainModelList],
        connection_property: ViewPropertyId | None = None,
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder, result_cls, result_list_cls)
        from_ = self._builder.get_from()
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    filter=filter_,
                ),
                max_retrieve_limit=limit,
                view_id=self._view_id,
                connection_property=connection_property,
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
    ) -> AlertQueryAPI[T_DomainModel, T_DomainModelList]:
        """Query along the alert edges of the partial bid matrix information.

        Args:
            min_time:
            max_time:
            workflow_execution_id:
            workflow_execution_id_prefix:
            title:
            title_prefix:
            description:
            description_prefix:
            severity:
            severity_prefix:
            alert_type:
            alert_type_prefix:
            min_status_code:
            max_status_code:
            calculation_run:
            calculation_run_prefix:
            external_id_prefix:
            space:
            external_id_prefix_edge:
            space_edge:
            filter: (Advanced) Filter applied to node. If the filtering available in the
                above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of alert edges to return.
                Defaults to 3. Set to -1, float("inf") or None to return all items.
            retrieve_power_asset: Whether to retrieve the power asset
                for each partial bid matrix information or not.
            retrieve_partial_bid_configuration: Whether to retrieve the partial bid configuration
                for each partial bid matrix information or not.

        Returns:
            AlertQueryAPI: The query API for the alert.
        """
        from .alert_query import AlertQueryAPI

        # from is a string as we added a node query step in the __init__ method
        from_ = cast(str, self._builder.get_from())
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "calculationIssue"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                max_retrieve_limit=limit,
                connection_property=ViewPropertyId(self._view_id, "alerts"),
            )
        )

        view_id = AlertQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filter = _create_alert_filter(
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
        return (AlertQueryAPI(
            self._client,
            self._builder,
            self._result_cls,
            self._result_list_cls,
            ViewPropertyId(self._view_id, "end_node"),
            node_filter,
            limit,
        ))
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
    ) -> BidMatrixQueryAPI[T_DomainModel, T_DomainModelList]:
        """Query along the underlying bid matrice edges of the partial bid matrix information.

        Args:
            state:
            state_prefix:
            external_id_prefix:
            space:
            external_id_prefix_edge:
            space_edge:
            filter: (Advanced) Filter applied to node. If the filtering available in the
                above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of underlying bid matrice edges to return.
                Defaults to 3. Set to -1, float("inf") or None to return all items.
            retrieve_power_asset: Whether to retrieve the power asset
                for each partial bid matrix information or not.
            retrieve_partial_bid_configuration: Whether to retrieve the partial bid configuration
                for each partial bid matrix information or not.

        Returns:
            BidMatrixQueryAPI: The query API for the bid matrix.
        """
        from .bid_matrix_query import BidMatrixQueryAPI

        # from is a string as we added a node query step in the __init__ method
        from_ = cast(str, self._builder.get_from())
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "intermediateBidMatrix"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                max_retrieve_limit=limit,
                connection_property=ViewPropertyId(self._view_id, "underlyingBidMatrices"),
            )
        )

        view_id = BidMatrixQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filter = _create_bid_matrix_filter(
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
        return (BidMatrixQueryAPI(
            self._client,
            self._builder,
            self._result_cls,
            self._result_list_cls,
            ViewPropertyId(self._view_id, "end_node"),
            node_filter,
            limit,
        ))

    def query(
        self,
        retrieve_power_asset: bool = False,
        retrieve_partial_bid_configuration: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_power_asset: Whether to retrieve the
                power asset for each
                partial bid matrix information or not.
            retrieve_partial_bid_configuration: Whether to retrieve the
                partial bid configuration for each
                partial bid matrix information or not.

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
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("powerAsset"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[PowerAsset._view_id]),
                ),
                view_id=PowerAsset._view_id,
                connection_property=ViewPropertyId(self._view_id, "powerAsset"),
            ),
        )
    def _query_append_partial_bid_configuration(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("partialBidConfiguration"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[PartialBidConfiguration._view_id]),
                ),
                view_id=PartialBidConfiguration._view_id,
                connection_property=ViewPropertyId(self._view_id, "partialBidConfiguration"),
            ),
        )
