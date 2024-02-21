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
    ModelTemplate,
    ModelTemplateWrite,
    ModelTemplateFields,
    ModelTemplateList,
    ModelTemplateWriteList,
    ModelTemplateTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._model_template import (
    _MODELTEMPLATE_PROPERTIES_BY_FIELD,
    _create_model_template_filter,
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
from .model_template_base_mappings import ModelTemplateBaseMappingsAPI
from .model_template_query import ModelTemplateQueryAPI


class ModelTemplateAPI(NodeAPI[ModelTemplate, ModelTemplateWrite, ModelTemplateList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[ModelTemplate]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ModelTemplate,
            class_list=ModelTemplateList,
            class_write_list=ModelTemplateWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.base_mappings_edge = ModelTemplateBaseMappingsAPI(client)

    def __call__(
        self,
        cog_shop_version: str | list[str] | None = None,
        cog_shop_version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        watercourse: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ModelTemplateQueryAPI[ModelTemplateList]:
        """Query starting at model templates.

        Args:
            cog_shop_version: The cog shop version to filter on.
            cog_shop_version_prefix: The prefix of the cog shop version to filter on.
            shop_version: The shop version to filter on.
            shop_version_prefix: The prefix of the shop version to filter on.
            watercourse: The watercourse to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of model templates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for model templates.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_model_template_filter(
            self._view_id,
            cog_shop_version,
            cog_shop_version_prefix,
            shop_version,
            shop_version_prefix,
            watercourse,
            source,
            source_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(ModelTemplateList)
        return ModelTemplateQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        model_template: ModelTemplateWrite | Sequence[ModelTemplateWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) model templates.

        Note: This method iterates through all nodes and timeseries linked to model_template and creates them including the edges
        between the nodes. For example, if any of `base_mappings` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            model_template: Model template or sequence of model templates to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new model_template:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import ModelTemplateWrite
                >>> client = PowerOpsModelsV1Client()
                >>> model_template = ModelTemplateWrite(external_id="my_model_template", ...)
                >>> result = client.model_template.apply(model_template)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.model_template.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(model_template, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more model template.

        Args:
            external_id: External id of the model template to delete.
            space: The space where all the model template are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete model_template by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.model_template.delete("my_model_template")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.model_template.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> ModelTemplate | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> ModelTemplateList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ModelTemplate | ModelTemplateList | None:
        """Retrieve one or more model templates by id(s).

        Args:
            external_id: External id or list of external ids of the model templates.
            space: The space where all the model templates are located.

        Returns:
            The requested model templates.

        Examples:

            Retrieve model_template by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> model_template = client.model_template.retrieve("my_model_template")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.base_mappings_edge,
                    "base_mappings",
                    dm.DirectRelationReference("sp_powerops_types", "ModelTemplate.baseMappings"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "Mapping", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: ModelTemplateTextFields | Sequence[ModelTemplateTextFields] | None = None,
        cog_shop_version: str | list[str] | None = None,
        cog_shop_version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        watercourse: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ModelTemplateList:
        """Search model templates

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            cog_shop_version: The cog shop version to filter on.
            cog_shop_version_prefix: The prefix of the cog shop version to filter on.
            shop_version: The shop version to filter on.
            shop_version_prefix: The prefix of the shop version to filter on.
            watercourse: The watercourse to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of model templates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results model templates matching the query.

        Examples:

           Search for 'my_model_template' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> model_templates = client.model_template.search('my_model_template')

        """
        filter_ = _create_model_template_filter(
            self._view_id,
            cog_shop_version,
            cog_shop_version_prefix,
            shop_version,
            shop_version_prefix,
            watercourse,
            source,
            source_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _MODELTEMPLATE_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: ModelTemplateFields | Sequence[ModelTemplateFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: ModelTemplateTextFields | Sequence[ModelTemplateTextFields] | None = None,
        cog_shop_version: str | list[str] | None = None,
        cog_shop_version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        watercourse: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
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
        property: ModelTemplateFields | Sequence[ModelTemplateFields] | None = None,
        group_by: ModelTemplateFields | Sequence[ModelTemplateFields] = None,
        query: str | None = None,
        search_properties: ModelTemplateTextFields | Sequence[ModelTemplateTextFields] | None = None,
        cog_shop_version: str | list[str] | None = None,
        cog_shop_version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        watercourse: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
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
        property: ModelTemplateFields | Sequence[ModelTemplateFields] | None = None,
        group_by: ModelTemplateFields | Sequence[ModelTemplateFields] | None = None,
        query: str | None = None,
        search_property: ModelTemplateTextFields | Sequence[ModelTemplateTextFields] | None = None,
        cog_shop_version: str | list[str] | None = None,
        cog_shop_version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        watercourse: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across model templates

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            cog_shop_version: The cog shop version to filter on.
            cog_shop_version_prefix: The prefix of the cog shop version to filter on.
            shop_version: The shop version to filter on.
            shop_version_prefix: The prefix of the shop version to filter on.
            watercourse: The watercourse to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of model templates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count model templates in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.model_template.aggregate("count", space="my_space")

        """

        filter_ = _create_model_template_filter(
            self._view_id,
            cog_shop_version,
            cog_shop_version_prefix,
            shop_version,
            shop_version_prefix,
            watercourse,
            source,
            source_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _MODELTEMPLATE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ModelTemplateFields,
        interval: float,
        query: str | None = None,
        search_property: ModelTemplateTextFields | Sequence[ModelTemplateTextFields] | None = None,
        cog_shop_version: str | list[str] | None = None,
        cog_shop_version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        watercourse: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for model templates

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            cog_shop_version: The cog shop version to filter on.
            cog_shop_version_prefix: The prefix of the cog shop version to filter on.
            shop_version: The shop version to filter on.
            shop_version_prefix: The prefix of the shop version to filter on.
            watercourse: The watercourse to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of model templates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_model_template_filter(
            self._view_id,
            cog_shop_version,
            cog_shop_version_prefix,
            shop_version,
            shop_version_prefix,
            watercourse,
            source,
            source_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _MODELTEMPLATE_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        cog_shop_version: str | list[str] | None = None,
        cog_shop_version_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        watercourse: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> ModelTemplateList:
        """List/filter model templates

        Args:
            cog_shop_version: The cog shop version to filter on.
            cog_shop_version_prefix: The prefix of the cog shop version to filter on.
            shop_version: The shop version to filter on.
            shop_version_prefix: The prefix of the shop version to filter on.
            watercourse: The watercourse to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of model templates to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `base_mappings` external ids for the model templates. Defaults to True.

        Returns:
            List of requested model templates

        Examples:

            List model templates and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> model_templates = client.model_template.list(limit=5)

        """
        filter_ = _create_model_template_filter(
            self._view_id,
            cog_shop_version,
            cog_shop_version_prefix,
            shop_version,
            shop_version_prefix,
            watercourse,
            source,
            source_prefix,
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
                    self.base_mappings_edge,
                    "base_mappings",
                    dm.DirectRelationReference("sp_powerops_types", "ModelTemplate.baseMappings"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "Mapping", "1"),
                ),
            ],
        )
