from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    ShopTriggerOutput,
    ShopTriggerInput,
    ShopResult,
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
from cognite.powerops.client._generated.v1._api._core import (
    QueryAPI,
    _create_edge_filter,
)

if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1._api.alert_query import AlertQueryAPI


class ShopTriggerOutputQueryAPI(QueryAPI[T_DomainModel, T_DomainModelList]):
    _view_id = dm.ViewId("power_ops_core", "ShopTriggerOutput", "1")

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
        retrieve_function_input: bool = False,
        retrieve_shop_result: bool = False,
    ) -> AlertQueryAPI[T_DomainModel, T_DomainModelList]:
        """Query along the alert edges of the shop trigger output.

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
            retrieve_function_input: Whether to retrieve the function input
                for each shop trigger output or not.
            retrieve_shop_result: Whether to retrieve the shop result
                for each shop trigger output or not.

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
        if retrieve_function_input:
            self._query_append_function_input(from_)
        if retrieve_shop_result:
            self._query_append_shop_result(from_)
        return (AlertQueryAPI(
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
        retrieve_function_input: bool = False,
        retrieve_shop_result: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_function_input: Whether to retrieve the
                function input for each
                shop trigger output or not.
            retrieve_shop_result: Whether to retrieve the
                shop result for each
                shop trigger output or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_function_input:
            self._query_append_function_input(from_)
        if retrieve_shop_result:
            self._query_append_shop_result(from_)
        return self._query()

    def _query_append_function_input(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("functionInput"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[ShopTriggerInput._view_id]),
                ),
                view_id=ShopTriggerInput._view_id,
                connection_property=ViewPropertyId(self._view_id, "functionInput"),
            ),
        )
    def _query_append_shop_result(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("shopResult"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[ShopResult._view_id]),
                ),
                view_id=ShopResult._view_id,
                connection_property=ViewPropertyId(self._view_id, "shopResult"),
            ),
        )
