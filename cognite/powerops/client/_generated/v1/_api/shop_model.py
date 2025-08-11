from __future__ import annotations

import warnings
from collections.abc import Iterator, Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite.powerops.client._generated.v1._api._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_CHUNK_SIZE,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryBuildStepFactory,
    QueryBuilder,
    QueryExecutor,
    QueryUnpacker,
    ViewPropertyId,
)
from cognite.powerops.client._generated.v1.data_classes._shop_model import (
    ShopModelQuery,
    _SHOPMODEL_PROPERTIES_BY_FIELD,
    _create_shop_model_filter,
)
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    ShopModel,
    ShopModelWrite,
    ShopModelFields,
    ShopModelList,
    ShopModelWriteList,
    ShopModelTextFields,
    ShopAttributeMapping,
    ShopFile,
)
from cognite.powerops.client._generated.v1._api.shop_model_cog_shop_files_config import ShopModelCogShopFilesConfigAPI
from cognite.powerops.client._generated.v1._api.shop_model_base_attribute_mappings import ShopModelBaseAttributeMappingsAPI


class ShopModelAPI(NodeAPI[ShopModel, ShopModelWrite, ShopModelList, ShopModelWriteList]):
    _view_id = dm.ViewId("power_ops_core", "ShopModel", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _SHOPMODEL_PROPERTIES_BY_FIELD
    _class_type = ShopModel
    _class_list = ShopModelList
    _class_write_list = ShopModelWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.cog_shop_files_config_edge = ShopModelCogShopFilesConfigAPI(client)
        self.base_attribute_mappings_edge = ShopModelBaseAttributeMappingsAPI(client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ShopModel | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ShopModelList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ShopModel | ShopModelList | None:
        """Retrieve one or more shop models by id(s).

        Args:
            external_id: External id or list of external ids of the shop models.
            space: The space where all the shop models are located.
            retrieve_connections: Whether to retrieve `cog_shop_files_config` and `base_attribute_mappings` for the shop
            models. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the
            identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            The requested shop models.

        Examples:

            Retrieve shop_model by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_model = client.shop_model.retrieve(
                ...     "my_shop_model"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_connections=retrieve_connections,
        )

    def search(
        self,
        query: str,
        properties: ShopModelTextFields | SequenceNotStr[ShopModelTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model_version: str | list[str] | None = None,
        model_version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ShopModelFields | SequenceNotStr[ShopModelFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ShopModelList:
        """Search shop models

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            model_version: The model version to filter on.
            model_version_prefix: The prefix of the model version to filter on.
            shop_version: The shop version to filter on.
            shop_version_prefix: The prefix of the shop version to filter on.
            min_penalty_limit: The minimum value of the penalty limit to filter on.
            max_penalty_limit: The maximum value of the penalty limit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop models to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results shop models matching the query.

        Examples:

           Search for 'my_shop_model' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_models = client.shop_model.search(
                ...     'my_shop_model'
                ... )

        """
        filter_ = _create_shop_model_filter(
            self._view_id,
            name,
            name_prefix,
            model_version,
            model_version_prefix,
            shop_version,
            shop_version_prefix,
            min_penalty_limit,
            max_penalty_limit,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            query=query,
            properties=properties,
            filter_=filter_,
            limit=limit,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
        )

    @overload
    def aggregate(
        self,
        aggregate: Aggregations | dm.aggregations.MetricAggregation,
        group_by: None = None,
        property: ShopModelFields | SequenceNotStr[ShopModelFields] | None = None,
        query: str | None = None,
        search_property: ShopModelTextFields | SequenceNotStr[ShopModelTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model_version: str | list[str] | None = None,
        model_version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.AggregatedNumberedValue: ...

    @overload
    def aggregate(
        self,
        aggregate: SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: None = None,
        property: ShopModelFields | SequenceNotStr[ShopModelFields] | None = None,
        query: str | None = None,
        search_property: ShopModelTextFields | SequenceNotStr[ShopModelTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model_version: str | list[str] | None = None,
        model_version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: ShopModelFields | SequenceNotStr[ShopModelFields],
        property: ShopModelFields | SequenceNotStr[ShopModelFields] | None = None,
        query: str | None = None,
        search_property: ShopModelTextFields | SequenceNotStr[ShopModelTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model_version: str | list[str] | None = None,
        model_version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: ShopModelFields | SequenceNotStr[ShopModelFields] | None = None,
        property: ShopModelFields | SequenceNotStr[ShopModelFields] | None = None,
        query: str | None = None,
        search_property: ShopModelTextFields | SequenceNotStr[ShopModelTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model_version: str | list[str] | None = None,
        model_version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across shop models

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            model_version: The model version to filter on.
            model_version_prefix: The prefix of the model version to filter on.
            shop_version: The shop version to filter on.
            shop_version_prefix: The prefix of the shop version to filter on.
            min_penalty_limit: The minimum value of the penalty limit to filter on.
            max_penalty_limit: The maximum value of the penalty limit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop models to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count shop models in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.shop_model.aggregate("count", space="my_space")

        """

        filter_ = _create_shop_model_filter(
            self._view_id,
            name,
            name_prefix,
            model_version,
            model_version_prefix,
            shop_version,
            shop_version_prefix,
            min_penalty_limit,
            max_penalty_limit,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            aggregate=aggregate,
            group_by=group_by,  # type: ignore[arg-type]
            properties=property,  # type: ignore[arg-type]
            query=query,
            search_properties=search_property,  # type: ignore[arg-type]
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: ShopModelFields,
        interval: float,
        query: str | None = None,
        search_property: ShopModelTextFields | SequenceNotStr[ShopModelTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model_version: str | list[str] | None = None,
        model_version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for shop models

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            model_version: The model version to filter on.
            model_version_prefix: The prefix of the model version to filter on.
            shop_version: The shop version to filter on.
            shop_version_prefix: The prefix of the shop version to filter on.
            min_penalty_limit: The minimum value of the penalty limit to filter on.
            max_penalty_limit: The maximum value of the penalty limit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop models to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_shop_model_filter(
            self._view_id,
            name,
            name_prefix,
            model_version,
            model_version_prefix,
            shop_version,
            shop_version_prefix,
            min_penalty_limit,
            max_penalty_limit,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            property,
            interval,
            query,
            search_property,  # type: ignore[arg-type]
            limit,
            filter_,
        )

    def select(self) -> ShopModelQuery:
        """Start selecting from shop models."""
        return ShopModelQuery(self._client)

    def _build(
        self,
        filter_: dm.Filter | None,
        limit: int | None,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
        chunk_size: int | None = None,
    ) -> QueryExecutor:
        builder = QueryBuilder()
        factory = QueryBuildStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(factory.root(
            filter=filter_,
            sort=sort,
            limit=limit,
            max_retrieve_batch_limit=chunk_size,
            has_container_fields=True,
        ))
        if retrieve_connections == "identifier" or retrieve_connections == "full":
            builder.extend(
                factory.from_edge(
                    ShopFile._view_id,
                    "outwards",
                    ViewPropertyId(self._view_id, "cogShopFilesConfig"),
                    include_end_node=retrieve_connections == "full",
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_edge(
                    ShopAttributeMapping._view_id,
                    "outwards",
                    ViewPropertyId(self._view_id, "baseAttributeMappings"),
                    include_end_node=retrieve_connections == "full",
                    has_container_fields=True,
                )
            )
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model_version: str | list[str] | None = None,
        model_version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[ShopModelList]:
        """Iterate over shop models

        Args:
            chunk_size: The number of shop models to return in each iteration. Defaults to 100.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            model_version: The model version to filter on.
            model_version_prefix: The prefix of the model version to filter on.
            shop_version: The shop version to filter on.
            shop_version_prefix: The prefix of the shop version to filter on.
            min_penalty_limit: The minimum value of the penalty limit to filter on.
            max_penalty_limit: The maximum value of the penalty limit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `cog_shop_files_config` and `base_attribute_mappings` for the shop
            models. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the
            identifier of the connected items, and 'full' will retrieve the full connected items.
            limit: Maximum number of shop models to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of shop models

        Examples:

            Iterate shop models in chunks of 100 up to 2000 items:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for shop_models in client.shop_model.iterate(chunk_size=100, limit=2000):
                ...     for shop_model in shop_models:
                ...         print(shop_model.external_id)

            Iterate shop models in chunks of 100 sorted by external_id in descending order:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for shop_models in client.shop_model.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for shop_model in shop_models:
                ...         print(shop_model.external_id)

            Iterate shop models in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for first_iteration in client.shop_model.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for shop_models in client.shop_model.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for shop_model in shop_models:
                ...         print(shop_model.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_shop_model_filter(
            self._view_id,
            name,
            name_prefix,
            model_version,
            model_version_prefix,
            shop_version,
            shop_version_prefix,
            min_penalty_limit,
            max_penalty_limit,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, retrieve_connections, cursors=cursors)

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model_version: str | list[str] | None = None,
        model_version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ShopModelFields | Sequence[ShopModelFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ShopModelList:
        """List/filter shop models

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            model_version: The model version to filter on.
            model_version_prefix: The prefix of the model version to filter on.
            shop_version: The shop version to filter on.
            shop_version_prefix: The prefix of the shop version to filter on.
            min_penalty_limit: The minimum value of the penalty limit to filter on.
            max_penalty_limit: The maximum value of the penalty limit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop models to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `cog_shop_files_config` and `base_attribute_mappings` for the shop
            models. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the
            identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested shop models

        Examples:

            List shop models and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_models = client.shop_model.list(limit=5)

        """
        filter_ = _create_shop_model_filter(
            self._view_id,
            name,
            name_prefix,
            model_version,
            model_version_prefix,
            shop_version,
            shop_version_prefix,
            min_penalty_limit,
            max_penalty_limit,
            external_id_prefix,
            space,
            filter,
        )
        sort_input =  self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        if retrieve_connections == "skip":
            return self._list(limit=limit,  filter=filter_, sort=sort_input)
        return self._query(filter_, limit, retrieve_connections, sort_input, "list")
