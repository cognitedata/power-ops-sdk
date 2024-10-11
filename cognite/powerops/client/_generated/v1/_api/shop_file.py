from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    ShopFile,
    ShopFileWrite,
    ShopFileFields,
    ShopFileList,
    ShopFileWriteList,
    ShopFileTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._shop_file import (
    _SHOPFILE_PROPERTIES_BY_FIELD,
    _create_shop_file_filter,
)
from ._core import DEFAULT_LIMIT_READ, DEFAULT_QUERY_LIMIT, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .shop_file_query import ShopFileQueryAPI


class ShopFileAPI(NodeAPI[ShopFile, ShopFileWrite, ShopFileList, ShopFileWriteList]):
    _view_id = dm.ViewId("power_ops_core", "ShopFile", "1")
    _properties_by_field = _SHOPFILE_PROPERTIES_BY_FIELD
    _class_type = ShopFile
    _class_list = ShopFileList
    _class_write_list = ShopFileWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)


    def __call__(
            self,
            name: str | list[str] | None = None,
            name_prefix: str | None = None,
            label: str | list[str] | None = None,
            label_prefix: str | None = None,
            file_reference_prefix: str | list[str] | None = None,
            file_reference_prefix_prefix: str | None = None,
            min_order: int | None = None,
            max_order: int | None = None,
            is_ascii: bool | None = None,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit: int = DEFAULT_QUERY_LIMIT,
            filter: dm.Filter | None = None,
    ) -> ShopFileQueryAPI[ShopFileList]:
        """Query starting at shop files.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            label: The label to filter on.
            label_prefix: The prefix of the label to filter on.
            file_reference_prefix: The file reference prefix to filter on.
            file_reference_prefix_prefix: The prefix of the file reference prefix to filter on.
            min_order: The minimum value of the order to filter on.
            max_order: The maximum value of the order to filter on.
            is_ascii: The is ascii to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop files to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for shop files.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_shop_file_filter(
            self._view_id,
            name,
            name_prefix,
            label,
            label_prefix,
            file_reference_prefix,
            file_reference_prefix_prefix,
            min_order,
            max_order,
            is_ascii,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(ShopFileList)
        return ShopFileQueryAPI(self._client, builder, filter_, limit)


    def apply(
        self,
        shop_file: ShopFileWrite | Sequence[ShopFileWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) shop files.

        Args:
            shop_file: Shop file or sequence of shop files to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new shop_file:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import ShopFileWrite
                >>> client = PowerOpsModelsV1Client()
                >>> shop_file = ShopFileWrite(external_id="my_shop_file", ...)
                >>> result = client.shop_file.apply(shop_file)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.shop_file.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(shop_file, replace, write_none)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> dm.InstancesDeleteResult:
        """Delete one or more shop file.

        Args:
            external_id: External id of the shop file to delete.
            space: The space where all the shop file are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete shop_file by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.shop_file.delete("my_shop_file")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.shop_file.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> ShopFile | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> ShopFileList:
        ...

    def retrieve(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> ShopFile | ShopFileList | None:
        """Retrieve one or more shop files by id(s).

        Args:
            external_id: External id or list of external ids of the shop files.
            space: The space where all the shop files are located.

        Returns:
            The requested shop files.

        Examples:

            Retrieve shop_file by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_file = client.shop_file.retrieve("my_shop_file")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: ShopFileTextFields | SequenceNotStr[ShopFileTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        label: str | list[str] | None = None,
        label_prefix: str | None = None,
        file_reference_prefix: str | list[str] | None = None,
        file_reference_prefix_prefix: str | None = None,
        min_order: int | None = None,
        max_order: int | None = None,
        is_ascii: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ShopFileFields | SequenceNotStr[ShopFileFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ShopFileList:
        """Search shop files

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            label: The label to filter on.
            label_prefix: The prefix of the label to filter on.
            file_reference_prefix: The file reference prefix to filter on.
            file_reference_prefix_prefix: The prefix of the file reference prefix to filter on.
            min_order: The minimum value of the order to filter on.
            max_order: The maximum value of the order to filter on.
            is_ascii: The is ascii to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop files to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results shop files matching the query.

        Examples:

           Search for 'my_shop_file' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_files = client.shop_file.search('my_shop_file')

        """
        filter_ = _create_shop_file_filter(
            self._view_id,
            name,
            name_prefix,
            label,
            label_prefix,
            file_reference_prefix,
            file_reference_prefix_prefix,
            min_order,
            max_order,
            is_ascii,
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
        property: ShopFileFields | SequenceNotStr[ShopFileFields] | None = None,
        query: str | None = None,
        search_property: ShopFileTextFields | SequenceNotStr[ShopFileTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        label: str | list[str] | None = None,
        label_prefix: str | None = None,
        file_reference_prefix: str | list[str] | None = None,
        file_reference_prefix_prefix: str | None = None,
        min_order: int | None = None,
        max_order: int | None = None,
        is_ascii: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.AggregatedNumberedValue:
        ...

    @overload
    def aggregate(
        self,
        aggregate: SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: None = None,
        property: ShopFileFields | SequenceNotStr[ShopFileFields] | None = None,
        query: str | None = None,
        search_property: ShopFileTextFields | SequenceNotStr[ShopFileTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        label: str | list[str] | None = None,
        label_prefix: str | None = None,
        file_reference_prefix: str | list[str] | None = None,
        file_reference_prefix_prefix: str | None = None,
        min_order: int | None = None,
        max_order: int | None = None,
        is_ascii: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: ShopFileFields | SequenceNotStr[ShopFileFields],
        property: ShopFileFields | SequenceNotStr[ShopFileFields] | None = None,
        query: str | None = None,
        search_property: ShopFileTextFields | SequenceNotStr[ShopFileTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        label: str | list[str] | None = None,
        label_prefix: str | None = None,
        file_reference_prefix: str | list[str] | None = None,
        file_reference_prefix_prefix: str | None = None,
        min_order: int | None = None,
        max_order: int | None = None,
        is_ascii: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: ShopFileFields | SequenceNotStr[ShopFileFields] | None = None,
        property: ShopFileFields | SequenceNotStr[ShopFileFields] | None = None,
        query: str | None = None,
        search_property: ShopFileTextFields | SequenceNotStr[ShopFileTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        label: str | list[str] | None = None,
        label_prefix: str | None = None,
        file_reference_prefix: str | list[str] | None = None,
        file_reference_prefix_prefix: str | None = None,
        min_order: int | None = None,
        max_order: int | None = None,
        is_ascii: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across shop files

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            label: The label to filter on.
            label_prefix: The prefix of the label to filter on.
            file_reference_prefix: The file reference prefix to filter on.
            file_reference_prefix_prefix: The prefix of the file reference prefix to filter on.
            min_order: The minimum value of the order to filter on.
            max_order: The maximum value of the order to filter on.
            is_ascii: The is ascii to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop files to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count shop files in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.shop_file.aggregate("count", space="my_space")

        """

        filter_ = _create_shop_file_filter(
            self._view_id,
            name,
            name_prefix,
            label,
            label_prefix,
            file_reference_prefix,
            file_reference_prefix_prefix,
            min_order,
            max_order,
            is_ascii,
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
        property: ShopFileFields,
        interval: float,
        query: str | None = None,
        search_property: ShopFileTextFields | SequenceNotStr[ShopFileTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        label: str | list[str] | None = None,
        label_prefix: str | None = None,
        file_reference_prefix: str | list[str] | None = None,
        file_reference_prefix_prefix: str | None = None,
        min_order: int | None = None,
        max_order: int | None = None,
        is_ascii: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for shop files

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            label: The label to filter on.
            label_prefix: The prefix of the label to filter on.
            file_reference_prefix: The file reference prefix to filter on.
            file_reference_prefix_prefix: The prefix of the file reference prefix to filter on.
            min_order: The minimum value of the order to filter on.
            max_order: The maximum value of the order to filter on.
            is_ascii: The is ascii to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop files to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_shop_file_filter(
            self._view_id,
            name,
            name_prefix,
            label,
            label_prefix,
            file_reference_prefix,
            file_reference_prefix_prefix,
            min_order,
            max_order,
            is_ascii,
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


    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        label: str | list[str] | None = None,
        label_prefix: str | None = None,
        file_reference_prefix: str | list[str] | None = None,
        file_reference_prefix_prefix: str | None = None,
        min_order: int | None = None,
        max_order: int | None = None,
        is_ascii: bool | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ShopFileFields | Sequence[ShopFileFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ShopFileList:
        """List/filter shop files

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            label: The label to filter on.
            label_prefix: The prefix of the label to filter on.
            file_reference_prefix: The file reference prefix to filter on.
            file_reference_prefix_prefix: The prefix of the file reference prefix to filter on.
            min_order: The minimum value of the order to filter on.
            max_order: The maximum value of the order to filter on.
            is_ascii: The is ascii to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop files to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested shop files

        Examples:

            List shop files and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_files = client.shop_file.list(limit=5)

        """
        filter_ = _create_shop_file_filter(
            self._view_id,
            name,
            name_prefix,
            label,
            label_prefix,
            file_reference_prefix,
            file_reference_prefix_prefix,
            min_order,
            max_order,
            is_ascii,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(
            limit=limit,
            filter=filter_,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
        )
