from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    ShopScenario,
    ShopModel,
    ShopCommands,
)
from cognite.powerops.client._generated.v1.data_classes._shop_output_time_series_definition import (
    ShopOutputTimeSeriesDefinition,
    _create_shop_output_time_series_definition_filter,
)
from cognite.powerops.client._generated.v1.data_classes._shop_attribute_mapping import (
    ShopAttributeMapping,
    _create_shop_attribute_mapping_filter,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .shop_output_time_series_definition_query import ShopOutputTimeSeriesDefinitionQueryAPI
    from .shop_attribute_mapping_query import ShopAttributeMappingQueryAPI



class ShopScenarioQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("power_ops_core", "ShopScenario", "1")

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
                name=self._builder.next_name("shop_scenario"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_id, ["*"])]),
                result_cls=ShopScenario,
                max_retrieve_limit=limit,
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
    ) -> ShopOutputTimeSeriesDefinitionQueryAPI[T_DomainModelList]:
        """Query along the output definition edges of the shop scenario.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            object_type: The object type to filter on.
            object_type_prefix: The prefix of the object type to filter on.
            object_name: The object name to filter on.
            object_name_prefix: The prefix of the object name to filter on.
            attribute_name: The attribute name to filter on.
            attribute_name_prefix: The prefix of the attribute name to filter on.
            unit: The unit to filter on.
            unit_prefix: The prefix of the unit to filter on.
            is_step: The is step to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of output definition edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_model: Whether to retrieve the model for each shop scenario or not.
            retrieve_commands: Whether to retrieve the command for each shop scenario or not.

        Returns:
            ShopOutputTimeSeriesDefinitionQueryAPI: The query API for the shop output time series definition.
        """
        from .shop_output_time_series_definition_query import ShopOutputTimeSeriesDefinitionQueryAPI

        from_ = self._builder[-1].name
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "ShopOutputTimeSeriesDefinition"),

            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("output_definition"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )

        view_id = ShopOutputTimeSeriesDefinitionQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_shop_output_time_series_definition_filter(
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
        return ShopOutputTimeSeriesDefinitionQueryAPI(self._client, self._builder, node_filer, limit)

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
    ) -> ShopAttributeMappingQueryAPI[T_DomainModelList]:
        """Query along the attribute mappings override edges of the shop scenario.

        Args:
            object_type: The object type to filter on.
            object_type_prefix: The prefix of the object type to filter on.
            object_name: The object name to filter on.
            object_name_prefix: The prefix of the object name to filter on.
            attribute_name: The attribute name to filter on.
            attribute_name_prefix: The prefix of the attribute name to filter on.
            retrieve: The retrieve to filter on.
            retrieve_prefix: The prefix of the retrieve to filter on.
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of attribute mappings override edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_model: Whether to retrieve the model for each shop scenario or not.
            retrieve_commands: Whether to retrieve the command for each shop scenario or not.

        Returns:
            ShopAttributeMappingQueryAPI: The query API for the shop attribute mapping.
        """
        from .shop_attribute_mapping_query import ShopAttributeMappingQueryAPI

        from_ = self._builder[-1].name
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "ShopAttributeMapping"),

            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("attribute_mappings_override"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )

        view_id = ShopAttributeMappingQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_shop_attribute_mapping_filter(
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
        return ShopAttributeMappingQueryAPI(self._client, self._builder, node_filer, limit)

    def query(
        self,
        retrieve_model: bool = False,
        retrieve_commands: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_model: Whether to retrieve the model for each shop scenario or not.
            retrieve_commands: Whether to retrieve the command for each shop scenario or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_model:
            self._query_append_model(from_)
        if retrieve_commands:
            self._query_append_commands(from_)
        return self._query()

    def _query_append_model(self, from_: str) -> None:
        view_id = ShopModel._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("model"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("model"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=ShopModel,
                is_single_direct_relation=True,
            ),
        )

    def _query_append_commands(self, from_: str) -> None:
        view_id = ShopCommands._view_id
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("commands"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_id.as_property_ref("commands"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=ShopCommands,
                is_single_direct_relation=True,
            ),
        )
