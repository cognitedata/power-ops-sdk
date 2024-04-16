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
    PartialBidConfiguration,
    PartialBidConfigurationWrite,
    PartialBidConfigurationFields,
    PartialBidConfigurationList,
    PartialBidConfigurationWriteList,
    PartialBidConfigurationTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._partial_bid_configuration import (
    _PARTIALBIDCONFIGURATION_PROPERTIES_BY_FIELD,
    _create_partial_bid_configuration_filter,
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
from .partial_bid_configuration_query import PartialBidConfigurationQueryAPI


class PartialBidConfigurationAPI(
    NodeAPI[PartialBidConfiguration, PartialBidConfigurationWrite, PartialBidConfigurationList]
):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[PartialBidConfiguration]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=PartialBidConfiguration,
            class_list=PartialBidConfigurationList,
            class_write_list=PartialBidConfigurationWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        add_steps: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> PartialBidConfigurationQueryAPI[PartialBidConfigurationList]:
        """Query starting at partial bid configurations.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            power_asset: The power asset to filter on.
            add_steps: The add step to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial bid configurations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for partial bid configurations.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_partial_bid_configuration_filter(
            self._view_id,
            name,
            name_prefix,
            method,
            method_prefix,
            power_asset,
            add_steps,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(PartialBidConfigurationList)
        return PartialBidConfigurationQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        partial_bid_configuration: PartialBidConfigurationWrite | Sequence[PartialBidConfigurationWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) partial bid configurations.

        Args:
            partial_bid_configuration: Partial bid configuration or sequence of partial bid configurations to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new partial_bid_configuration:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import PartialBidConfigurationWrite
                >>> client = PowerOpsModelsV1Client()
                >>> partial_bid_configuration = PartialBidConfigurationWrite(external_id="my_partial_bid_configuration", ...)
                >>> result = client.partial_bid_configuration.apply(partial_bid_configuration)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.partial_bid_configuration.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(partial_bid_configuration, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more partial bid configuration.

        Args:
            external_id: External id of the partial bid configuration to delete.
            space: The space where all the partial bid configuration are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete partial_bid_configuration by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.partial_bid_configuration.delete("my_partial_bid_configuration")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.partial_bid_configuration.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> PartialBidConfiguration | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> PartialBidConfigurationList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> PartialBidConfiguration | PartialBidConfigurationList | None:
        """Retrieve one or more partial bid configurations by id(s).

        Args:
            external_id: External id or list of external ids of the partial bid configurations.
            space: The space where all the partial bid configurations are located.

        Returns:
            The requested partial bid configurations.

        Examples:

            Retrieve partial_bid_configuration by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> partial_bid_configuration = client.partial_bid_configuration.retrieve("my_partial_bid_configuration")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: PartialBidConfigurationTextFields | Sequence[PartialBidConfigurationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        add_steps: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PartialBidConfigurationList:
        """Search partial bid configurations

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            power_asset: The power asset to filter on.
            add_steps: The add step to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial bid configurations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results partial bid configurations matching the query.

        Examples:

           Search for 'my_partial_bid_configuration' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> partial_bid_configurations = client.partial_bid_configuration.search('my_partial_bid_configuration')

        """
        filter_ = _create_partial_bid_configuration_filter(
            self._view_id,
            name,
            name_prefix,
            method,
            method_prefix,
            power_asset,
            add_steps,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _PARTIALBIDCONFIGURATION_PROPERTIES_BY_FIELD, properties, filter_, limit
        )

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: PartialBidConfigurationFields | Sequence[PartialBidConfigurationFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: (
            PartialBidConfigurationTextFields | Sequence[PartialBidConfigurationTextFields] | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        add_steps: bool | None = None,
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
        property: PartialBidConfigurationFields | Sequence[PartialBidConfigurationFields] | None = None,
        group_by: PartialBidConfigurationFields | Sequence[PartialBidConfigurationFields] = None,
        query: str | None = None,
        search_properties: (
            PartialBidConfigurationTextFields | Sequence[PartialBidConfigurationTextFields] | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        add_steps: bool | None = None,
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
        property: PartialBidConfigurationFields | Sequence[PartialBidConfigurationFields] | None = None,
        group_by: PartialBidConfigurationFields | Sequence[PartialBidConfigurationFields] | None = None,
        query: str | None = None,
        search_property: PartialBidConfigurationTextFields | Sequence[PartialBidConfigurationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        add_steps: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across partial bid configurations

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            power_asset: The power asset to filter on.
            add_steps: The add step to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial bid configurations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count partial bid configurations in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.partial_bid_configuration.aggregate("count", space="my_space")

        """

        filter_ = _create_partial_bid_configuration_filter(
            self._view_id,
            name,
            name_prefix,
            method,
            method_prefix,
            power_asset,
            add_steps,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _PARTIALBIDCONFIGURATION_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: PartialBidConfigurationFields,
        interval: float,
        query: str | None = None,
        search_property: PartialBidConfigurationTextFields | Sequence[PartialBidConfigurationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        add_steps: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for partial bid configurations

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            power_asset: The power asset to filter on.
            add_steps: The add step to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial bid configurations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_partial_bid_configuration_filter(
            self._view_id,
            name,
            name_prefix,
            method,
            method_prefix,
            power_asset,
            add_steps,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _PARTIALBIDCONFIGURATION_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        add_steps: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PartialBidConfigurationList:
        """List/filter partial bid configurations

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            power_asset: The power asset to filter on.
            add_steps: The add step to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of partial bid configurations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested partial bid configurations

        Examples:

            List partial bid configurations and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> partial_bid_configurations = client.partial_bid_configuration.list(limit=5)

        """
        filter_ = _create_partial_bid_configuration_filter(
            self._view_id,
            name,
            name_prefix,
            method,
            method_prefix,
            power_asset,
            add_steps,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
