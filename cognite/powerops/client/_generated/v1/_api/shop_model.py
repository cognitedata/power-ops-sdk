from __future__ import annotations

from collections.abc import Sequence
from typing import overload
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    ShopModel,
    ShopModelWrite,
    ShopModelFields,
    ShopModelList,
    ShopModelWriteList,
    ShopModelTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._shop_model import (
    _SHOPMODEL_PROPERTIES_BY_FIELD,
    _create_shop_model_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_QUERY_LIMIT,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
    QueryStep,
    QueryBuilder,
)
from .shop_model_base_attribute_mappings import ShopModelBaseAttributeMappingsAPI
from .shop_model_query import ShopModelQueryAPI


class ShopModelAPI(NodeAPI[ShopModel, ShopModelWrite, ShopModelList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[ShopModel]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ShopModel,
            class_list=ShopModelList,
            class_write_list=ShopModelWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.base_attribute_mappings_edge = ShopModelBaseAttributeMappingsAPI(client)

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        version_: str | list[str] | None = None,
        version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ShopModelQueryAPI[ShopModelList]:
        """Query starting at shop models.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            version_: The version to filter on.
            version_prefix: The prefix of the version to filter on.
            shop_version: The shop version to filter on.
            shop_version_prefix: The prefix of the shop version to filter on.
            min_penalty_limit: The minimum value of the penalty limit to filter on.
            max_penalty_limit: The maximum value of the penalty limit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop models to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for shop models.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_shop_model_filter(
            self._view_id,
            name,
            name_prefix,
            version_,
            version_prefix,
            shop_version,
            shop_version_prefix,
            min_penalty_limit,
            max_penalty_limit,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(ShopModelList)
        return ShopModelQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        shop_model: ShopModelWrite | Sequence[ShopModelWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) shop models.

        Note: This method iterates through all nodes and timeseries linked to shop_model and creates them including the edges
        between the nodes. For example, if any of `base_attribute_mappings` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            shop_model: Shop model or sequence of shop models to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new shop_model:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import ShopModelWrite
                >>> client = PowerOpsModelsV1Client()
                >>> shop_model = ShopModelWrite(external_id="my_shop_model", ...)
                >>> result = client.shop_model.apply(shop_model)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.shop_model.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(shop_model, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more shop model.

        Args:
            external_id: External id of the shop model to delete.
            space: The space where all the shop model are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete shop_model by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.shop_model.delete("my_shop_model")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.shop_model.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> ShopModel | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> ShopModelList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ShopModel | ShopModelList | None:
        """Retrieve one or more shop models by id(s).

        Args:
            external_id: External id or list of external ids of the shop models.
            space: The space where all the shop models are located.

        Returns:
            The requested shop models.

        Examples:

            Retrieve shop_model by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_model = client.shop_model.retrieve("my_shop_model")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.base_attribute_mappings_edge,
                    "base_attribute_mappings",
                    dm.DirectRelationReference("sp_power_ops_types", "ShopModel.baseAttributeMappings"),
                    "outwards",
                    dm.ViewId("sp_power_ops_models", "ShopAttributeMapping", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: ShopModelTextFields | Sequence[ShopModelTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        version_: str | list[str] | None = None,
        version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ShopModelList:
        """Search shop models

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            version_: The version to filter on.
            version_prefix: The prefix of the version to filter on.
            shop_version: The shop version to filter on.
            shop_version_prefix: The prefix of the shop version to filter on.
            min_penalty_limit: The minimum value of the penalty limit to filter on.
            max_penalty_limit: The maximum value of the penalty limit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop models to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results shop models matching the query.

        Examples:

           Search for 'my_shop_model' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_models = client.shop_model.search('my_shop_model')

        """
        filter_ = _create_shop_model_filter(
            self._view_id,
            name,
            name_prefix,
            version_,
            version_prefix,
            shop_version,
            shop_version_prefix,
            min_penalty_limit,
            max_penalty_limit,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _SHOPMODEL_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: ShopModelFields | Sequence[ShopModelFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: ShopModelTextFields | Sequence[ShopModelTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        version_: str | list[str] | None = None,
        version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: ShopModelFields | Sequence[ShopModelFields] | None = None,
        group_by: ShopModelFields | Sequence[ShopModelFields] = None,
        query: str | None = None,
        search_properties: ShopModelTextFields | Sequence[ShopModelTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        version_: str | list[str] | None = None,
        version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: ShopModelFields | Sequence[ShopModelFields] | None = None,
        group_by: ShopModelFields | Sequence[ShopModelFields] | None = None,
        query: str | None = None,
        search_property: ShopModelTextFields | Sequence[ShopModelTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        version_: str | list[str] | None = None,
        version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across shop models

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            version_: The version to filter on.
            version_prefix: The prefix of the version to filter on.
            shop_version: The shop version to filter on.
            shop_version_prefix: The prefix of the shop version to filter on.
            min_penalty_limit: The minimum value of the penalty limit to filter on.
            max_penalty_limit: The maximum value of the penalty limit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop models to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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
            version_,
            version_prefix,
            shop_version,
            shop_version_prefix,
            min_penalty_limit,
            max_penalty_limit,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _SHOPMODEL_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ShopModelFields,
        interval: float,
        query: str | None = None,
        search_property: ShopModelTextFields | Sequence[ShopModelTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        version_: str | list[str] | None = None,
        version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
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
            version_: The version to filter on.
            version_prefix: The prefix of the version to filter on.
            shop_version: The shop version to filter on.
            shop_version_prefix: The prefix of the shop version to filter on.
            min_penalty_limit: The minimum value of the penalty limit to filter on.
            max_penalty_limit: The maximum value of the penalty limit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop models to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_shop_model_filter(
            self._view_id,
            name,
            name_prefix,
            version_,
            version_prefix,
            shop_version,
            shop_version_prefix,
            min_penalty_limit,
            max_penalty_limit,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _SHOPMODEL_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        version_: str | list[str] | None = None,
        version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> ShopModelList:
        """List/filter shop models

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            version_: The version to filter on.
            version_prefix: The prefix of the version to filter on.
            shop_version: The shop version to filter on.
            shop_version_prefix: The prefix of the shop version to filter on.
            min_penalty_limit: The minimum value of the penalty limit to filter on.
            max_penalty_limit: The maximum value of the penalty limit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop models to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `base_attribute_mappings` external ids for the shop models. Defaults to True.

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
            version_,
            version_prefix,
            shop_version,
            shop_version_prefix,
            min_penalty_limit,
            max_penalty_limit,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.base_attribute_mappings_edge,
                    "base_attribute_mappings",
                    dm.DirectRelationReference("sp_power_ops_types", "ShopModel.baseAttributeMappings"),
                    "outwards",
                    dm.ViewId("sp_power_ops_models", "ShopAttributeMapping", "1"),
                ),
            ],
        )
