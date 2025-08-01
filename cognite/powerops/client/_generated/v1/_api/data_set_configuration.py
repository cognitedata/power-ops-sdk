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
from cognite.powerops.client._generated.v1.data_classes._data_set_configuration import (
    DataSetConfigurationQuery,
    _DATASETCONFIGURATION_PROPERTIES_BY_FIELD,
    _create_data_set_configuration_filter,
)
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    DataSetConfiguration,
    DataSetConfigurationWrite,
    DataSetConfigurationFields,
    DataSetConfigurationList,
    DataSetConfigurationWriteList,
    DataSetConfigurationTextFields,
)


class DataSetConfigurationAPI(NodeAPI[DataSetConfiguration, DataSetConfigurationWrite, DataSetConfigurationList, DataSetConfigurationWriteList]):
    _view_id = dm.ViewId("power_ops_core", "DataSetConfiguration", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _DATASETCONFIGURATION_PROPERTIES_BY_FIELD
    _class_type = DataSetConfiguration
    _class_list = DataSetConfigurationList
    _class_write_list = DataSetConfigurationWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)


    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> DataSetConfiguration | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> DataSetConfigurationList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> DataSetConfiguration | DataSetConfigurationList | None:
        """Retrieve one or more data set configurations by id(s).

        Args:
            external_id: External id or list of external ids of the data set configurations.
            space: The space where all the data set configurations are located.

        Returns:
            The requested data set configurations.

        Examples:

            Retrieve data_set_configuration by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> data_set_configuration = client.data_set_configuration.retrieve(
                ...     "my_data_set_configuration"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
        )

    def search(
        self,
        query: str,
        properties: DataSetConfigurationTextFields | SequenceNotStr[DataSetConfigurationTextFields] | None = None,
        read_data_set: str | list[str] | None = None,
        read_data_set_prefix: str | None = None,
        write_data_set: str | list[str] | None = None,
        write_data_set_prefix: str | None = None,
        monitor_data_set: str | list[str] | None = None,
        monitor_data_set_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: DataSetConfigurationFields | SequenceNotStr[DataSetConfigurationFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> DataSetConfigurationList:
        """Search data set configurations

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            read_data_set: The read data set to filter on.
            read_data_set_prefix: The prefix of the read data set to filter on.
            write_data_set: The write data set to filter on.
            write_data_set_prefix: The prefix of the write data set to filter on.
            monitor_data_set: The monitor data set to filter on.
            monitor_data_set_prefix: The prefix of the monitor data set to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of data set configurations to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results data set configurations matching the query.

        Examples:

           Search for 'my_data_set_configuration' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> data_set_configurations = client.data_set_configuration.search(
                ...     'my_data_set_configuration'
                ... )

        """
        filter_ = _create_data_set_configuration_filter(
            self._view_id,
            read_data_set,
            read_data_set_prefix,
            write_data_set,
            write_data_set_prefix,
            monitor_data_set,
            monitor_data_set_prefix,
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
        property: DataSetConfigurationFields | SequenceNotStr[DataSetConfigurationFields] | None = None,
        query: str | None = None,
        search_property: DataSetConfigurationTextFields | SequenceNotStr[DataSetConfigurationTextFields] | None = None,
        read_data_set: str | list[str] | None = None,
        read_data_set_prefix: str | None = None,
        write_data_set: str | list[str] | None = None,
        write_data_set_prefix: str | None = None,
        monitor_data_set: str | list[str] | None = None,
        monitor_data_set_prefix: str | None = None,
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
        property: DataSetConfigurationFields | SequenceNotStr[DataSetConfigurationFields] | None = None,
        query: str | None = None,
        search_property: DataSetConfigurationTextFields | SequenceNotStr[DataSetConfigurationTextFields] | None = None,
        read_data_set: str | list[str] | None = None,
        read_data_set_prefix: str | None = None,
        write_data_set: str | list[str] | None = None,
        write_data_set_prefix: str | None = None,
        monitor_data_set: str | list[str] | None = None,
        monitor_data_set_prefix: str | None = None,
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
        group_by: DataSetConfigurationFields | SequenceNotStr[DataSetConfigurationFields],
        property: DataSetConfigurationFields | SequenceNotStr[DataSetConfigurationFields] | None = None,
        query: str | None = None,
        search_property: DataSetConfigurationTextFields | SequenceNotStr[DataSetConfigurationTextFields] | None = None,
        read_data_set: str | list[str] | None = None,
        read_data_set_prefix: str | None = None,
        write_data_set: str | list[str] | None = None,
        write_data_set_prefix: str | None = None,
        monitor_data_set: str | list[str] | None = None,
        monitor_data_set_prefix: str | None = None,
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
        group_by: DataSetConfigurationFields | SequenceNotStr[DataSetConfigurationFields] | None = None,
        property: DataSetConfigurationFields | SequenceNotStr[DataSetConfigurationFields] | None = None,
        query: str | None = None,
        search_property: DataSetConfigurationTextFields | SequenceNotStr[DataSetConfigurationTextFields] | None = None,
        read_data_set: str | list[str] | None = None,
        read_data_set_prefix: str | None = None,
        write_data_set: str | list[str] | None = None,
        write_data_set_prefix: str | None = None,
        monitor_data_set: str | list[str] | None = None,
        monitor_data_set_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across data set configurations

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            read_data_set: The read data set to filter on.
            read_data_set_prefix: The prefix of the read data set to filter on.
            write_data_set: The write data set to filter on.
            write_data_set_prefix: The prefix of the write data set to filter on.
            monitor_data_set: The monitor data set to filter on.
            monitor_data_set_prefix: The prefix of the monitor data set to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of data set configurations to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count data set configurations in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.data_set_configuration.aggregate("count", space="my_space")

        """

        filter_ = _create_data_set_configuration_filter(
            self._view_id,
            read_data_set,
            read_data_set_prefix,
            write_data_set,
            write_data_set_prefix,
            monitor_data_set,
            monitor_data_set_prefix,
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
        property: DataSetConfigurationFields,
        interval: float,
        query: str | None = None,
        search_property: DataSetConfigurationTextFields | SequenceNotStr[DataSetConfigurationTextFields] | None = None,
        read_data_set: str | list[str] | None = None,
        read_data_set_prefix: str | None = None,
        write_data_set: str | list[str] | None = None,
        write_data_set_prefix: str | None = None,
        monitor_data_set: str | list[str] | None = None,
        monitor_data_set_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for data set configurations

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            read_data_set: The read data set to filter on.
            read_data_set_prefix: The prefix of the read data set to filter on.
            write_data_set: The write data set to filter on.
            write_data_set_prefix: The prefix of the write data set to filter on.
            monitor_data_set: The monitor data set to filter on.
            monitor_data_set_prefix: The prefix of the monitor data set to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of data set configurations to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_data_set_configuration_filter(
            self._view_id,
            read_data_set,
            read_data_set_prefix,
            write_data_set,
            write_data_set_prefix,
            monitor_data_set,
            monitor_data_set_prefix,
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

    def select(self) -> DataSetConfigurationQuery:
        """Start selecting from data set configurations."""
        return DataSetConfigurationQuery(self._client)

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
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        read_data_set: str | list[str] | None = None,
        read_data_set_prefix: str | None = None,
        write_data_set: str | list[str] | None = None,
        write_data_set_prefix: str | None = None,
        monitor_data_set: str | list[str] | None = None,
        monitor_data_set_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[DataSetConfigurationList]:
        """Iterate over data set configurations

        Args:
            chunk_size: The number of data set configurations to return in each iteration. Defaults to 100.
            read_data_set: The read data set to filter on.
            read_data_set_prefix: The prefix of the read data set to filter on.
            write_data_set: The write data set to filter on.
            write_data_set_prefix: The prefix of the write data set to filter on.
            monitor_data_set: The monitor data set to filter on.
            monitor_data_set_prefix: The prefix of the monitor data set to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of data set configurations to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of data set configurations

        Examples:

            Iterate data set configurations in chunks of 100 up to 2000 items:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for data_set_configurations in client.data_set_configuration.iterate(chunk_size=100, limit=2000):
                ...     for data_set_configuration in data_set_configurations:
                ...         print(data_set_configuration.external_id)

            Iterate data set configurations in chunks of 100 sorted by external_id in descending order:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for data_set_configurations in client.data_set_configuration.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for data_set_configuration in data_set_configurations:
                ...         print(data_set_configuration.external_id)

            Iterate data set configurations in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for first_iteration in client.data_set_configuration.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for data_set_configurations in client.data_set_configuration.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for data_set_configuration in data_set_configurations:
                ...         print(data_set_configuration.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_data_set_configuration_filter(
            self._view_id,
            read_data_set,
            read_data_set_prefix,
            write_data_set,
            write_data_set_prefix,
            monitor_data_set,
            monitor_data_set_prefix,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, "skip", cursors=cursors)

    def list(
        self,
        read_data_set: str | list[str] | None = None,
        read_data_set_prefix: str | None = None,
        write_data_set: str | list[str] | None = None,
        write_data_set_prefix: str | None = None,
        monitor_data_set: str | list[str] | None = None,
        monitor_data_set_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: DataSetConfigurationFields | Sequence[DataSetConfigurationFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> DataSetConfigurationList:
        """List/filter data set configurations

        Args:
            read_data_set: The read data set to filter on.
            read_data_set_prefix: The prefix of the read data set to filter on.
            write_data_set: The write data set to filter on.
            write_data_set_prefix: The prefix of the write data set to filter on.
            monitor_data_set: The monitor data set to filter on.
            monitor_data_set_prefix: The prefix of the monitor data set to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of data set configurations to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested data set configurations

        Examples:

            List data set configurations and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> data_set_configurations = client.data_set_configuration.list(limit=5)

        """
        filter_ = _create_data_set_configuration_filter(
            self._view_id,
            read_data_set,
            read_data_set_prefix,
            write_data_set,
            write_data_set_prefix,
            monitor_data_set,
            monitor_data_set_prefix,
            external_id_prefix,
            space,
            filter,
        )
        sort_input =  self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        return self._list(limit=limit,  filter=filter_, sort=sort_input)
