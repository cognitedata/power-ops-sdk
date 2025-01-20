from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    ShopScenario,
    ShopModel,
    ShopCommands,
    ShopTimeResolution,
)
from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    ViewPropertyId,
    T_DomainModel,
    T_DomainModelList,
    QueryBuilder,
    QueryStep,
)
from cognite.powerops.client._generated.v1.data_classes._shop_output_time_series_definition import (
    _create_shop_output_time_series_definition_filter,
)
from cognite.powerops.client._generated.v1.data_classes._shop_attribute_mapping import (
    _create_shop_attribute_mapping_filter,
)
from cognite.powerops.client._generated.v1._api._core import (
    QueryAPI,
    _create_edge_filter,
)

if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1._api.shop_output_time_series_definition_query import ShopOutputTimeSeriesDefinitionQueryAPI
    from cognite.powerops.client._generated.v1._api.shop_attribute_mapping_query import ShopAttributeMappingQueryAPI


class ShopScenarioQueryAPI(QueryAPI[T_DomainModel, T_DomainModelList]):
    _view_id = dm.ViewId("power_ops_core", "ShopScenario", "1")

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
    def output_definition(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
        unit: str | list[str] | None = None,
        unit_prefix: str | None = None,
        is_step: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        retrieve_model: bool = False,
        retrieve_commands: bool = False,
        retrieve_time_resolution: bool = False,
    ) -> ShopOutputTimeSeriesDefinitionQueryAPI[T_DomainModel, T_DomainModelList]:
        """Query along the output definition edges of the shop scenario.

        Args:
            name:
            name_prefix:
            object_type:
            object_type_prefix:
            object_name:
            object_name_prefix:
            attribute_name:
            attribute_name_prefix:
            unit:
            unit_prefix:
            is_step:
            external_id_prefix:
            space:
            external_id_prefix_edge:
            space_edge:
            filter: (Advanced) Filter applied to node. If the filtering available in the
                above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of output definition edges to return.
                Defaults to 3. Set to -1, float("inf") or None to return all items.
            retrieve_model: Whether to retrieve the model
                for each shop scenario or not.
            retrieve_commands: Whether to retrieve the command
                for each shop scenario or not.
            retrieve_time_resolution: Whether to retrieve the time resolution
                for each shop scenario or not.

        Returns:
            ShopOutputTimeSeriesDefinitionQueryAPI: The query API for the shop output time series definition.
        """
        from .shop_output_time_series_definition_query import ShopOutputTimeSeriesDefinitionQueryAPI

        # from is a string as we added a node query step in the __init__ method
        from_ = cast(str, self._builder.get_from())
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "ShopOutputTimeSeriesDefinition"),
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
                connection_property=ViewPropertyId(self._view_id, "outputDefinition"),
            )
        )

        view_id = ShopOutputTimeSeriesDefinitionQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filter = _create_shop_output_time_series_definition_filter(
            view_id,
            name,
            name_prefix,
            object_type,
            object_type_prefix,
            object_name,
            object_name_prefix,
            attribute_name,
            attribute_name_prefix,
            unit,
            unit_prefix,
            is_step,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_model:
            self._query_append_model(from_)
        if retrieve_commands:
            self._query_append_commands(from_)
        if retrieve_time_resolution:
            self._query_append_time_resolution(from_)
        return (ShopOutputTimeSeriesDefinitionQueryAPI(
            self._client,
            self._builder,
            self._result_cls,
            self._result_list_cls,
            ViewPropertyId(self._view_id, "end_node"),
            node_filter,
            limit,
        ))
    def attribute_mappings_override(
        self,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        retrieve_model: bool = False,
        retrieve_commands: bool = False,
        retrieve_time_resolution: bool = False,
    ) -> ShopAttributeMappingQueryAPI[T_DomainModel, T_DomainModelList]:
        """Query along the attribute mappings override edges of the shop scenario.

        Args:
            object_type:
            object_type_prefix:
            object_name:
            object_name_prefix:
            attribute_name:
            attribute_name_prefix:
            retrieve:
            retrieve_prefix:
            aggregation:
            aggregation_prefix:
            external_id_prefix:
            space:
            external_id_prefix_edge:
            space_edge:
            filter: (Advanced) Filter applied to node. If the filtering available in the
                above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of attribute mappings override edges to return.
                Defaults to 3. Set to -1, float("inf") or None to return all items.
            retrieve_model: Whether to retrieve the model
                for each shop scenario or not.
            retrieve_commands: Whether to retrieve the command
                for each shop scenario or not.
            retrieve_time_resolution: Whether to retrieve the time resolution
                for each shop scenario or not.

        Returns:
            ShopAttributeMappingQueryAPI: The query API for the shop attribute mapping.
        """
        from .shop_attribute_mapping_query import ShopAttributeMappingQueryAPI

        # from is a string as we added a node query step in the __init__ method
        from_ = cast(str, self._builder.get_from())
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "ShopAttributeMapping"),
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
                connection_property=ViewPropertyId(self._view_id, "attributeMappingsOverride"),
            )
        )

        view_id = ShopAttributeMappingQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filter = _create_shop_attribute_mapping_filter(
            view_id,
            object_type,
            object_type_prefix,
            object_name,
            object_name_prefix,
            attribute_name,
            attribute_name_prefix,
            retrieve,
            retrieve_prefix,
            aggregation,
            aggregation_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_model:
            self._query_append_model(from_)
        if retrieve_commands:
            self._query_append_commands(from_)
        if retrieve_time_resolution:
            self._query_append_time_resolution(from_)
        return (ShopAttributeMappingQueryAPI(
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
        retrieve_model: bool = False,
        retrieve_commands: bool = False,
        retrieve_time_resolution: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_model: Whether to retrieve the
                model for each
                shop scenario or not.
            retrieve_commands: Whether to retrieve the
                command for each
                shop scenario or not.
            retrieve_time_resolution: Whether to retrieve the
                time resolution for each
                shop scenario or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_model:
            self._query_append_model(from_)
        if retrieve_commands:
            self._query_append_commands(from_)
        if retrieve_time_resolution:
            self._query_append_time_resolution(from_)
        return self._query()

    def _query_append_model(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("model"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[ShopModel._view_id]),
                ),
                view_id=ShopModel._view_id,
                connection_property=ViewPropertyId(self._view_id, "model"),
            ),
        )
    def _query_append_commands(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("commands"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[ShopCommands._view_id]),
                ),
                view_id=ShopCommands._view_id,
                connection_property=ViewPropertyId(self._view_id, "commands"),
            ),
        )
    def _query_append_time_resolution(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("timeResolution"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[ShopTimeResolution._view_id]),
                ),
                view_id=ShopTimeResolution._view_id,
                connection_property=ViewPropertyId(self._view_id, "timeResolution"),
            ),
        )
