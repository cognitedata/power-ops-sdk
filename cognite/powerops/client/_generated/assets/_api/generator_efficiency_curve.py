from __future__ import annotations

from collections.abc import Sequence
from typing import overload
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.assets.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.assets.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    GeneratorEfficiencyCurve,
    GeneratorEfficiencyCurveWrite,
    GeneratorEfficiencyCurveFields,
    GeneratorEfficiencyCurveList,
    GeneratorEfficiencyCurveWriteList,
)
from cognite.powerops.client._generated.assets.data_classes._generator_efficiency_curve import (
    _GENERATOREFFICIENCYCURVE_PROPERTIES_BY_FIELD,
    _create_generator_efficiency_curve_filter,
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
from .generator_efficiency_curve_query import GeneratorEfficiencyCurveQueryAPI


class GeneratorEfficiencyCurveAPI(
    NodeAPI[GeneratorEfficiencyCurve, GeneratorEfficiencyCurveWrite, GeneratorEfficiencyCurveList]
):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[GeneratorEfficiencyCurve]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=GeneratorEfficiencyCurve,
            class_list=GeneratorEfficiencyCurveList,
            class_write_list=GeneratorEfficiencyCurveWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        min_ref: float | None = None,
        max_ref: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> GeneratorEfficiencyCurveQueryAPI[GeneratorEfficiencyCurveList]:
        """Query starting at generator efficiency curves.

        Args:
            min_ref: The minimum value of the ref to filter on.
            max_ref: The maximum value of the ref to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generator efficiency curves to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for generator efficiency curves.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_generator_efficiency_curve_filter(
            self._view_id,
            min_ref,
            max_ref,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(GeneratorEfficiencyCurveList)
        return GeneratorEfficiencyCurveQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        generator_efficiency_curve: GeneratorEfficiencyCurveWrite | Sequence[GeneratorEfficiencyCurveWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) generator efficiency curves.

        Args:
            generator_efficiency_curve: Generator efficiency curve or sequence of generator efficiency curves to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new generator_efficiency_curve:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> from cognite.powerops.client._generated.assets.data_classes import GeneratorEfficiencyCurveWrite
                >>> client = PowerAssetAPI()
                >>> generator_efficiency_curve = GeneratorEfficiencyCurveWrite(external_id="my_generator_efficiency_curve", ...)
                >>> result = client.generator_efficiency_curve.apply(generator_efficiency_curve)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.generator_efficiency_curve.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(generator_efficiency_curve, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more generator efficiency curve.

        Args:
            external_id: External id of the generator efficiency curve to delete.
            space: The space where all the generator efficiency curve are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete generator_efficiency_curve by id:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> client.generator_efficiency_curve.delete("my_generator_efficiency_curve")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.generator_efficiency_curve.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> GeneratorEfficiencyCurve | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> GeneratorEfficiencyCurveList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> GeneratorEfficiencyCurve | GeneratorEfficiencyCurveList | None:
        """Retrieve one or more generator efficiency curves by id(s).

        Args:
            external_id: External id or list of external ids of the generator efficiency curves.
            space: The space where all the generator efficiency curves are located.

        Returns:
            The requested generator efficiency curves.

        Examples:

            Retrieve generator_efficiency_curve by id:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> generator_efficiency_curve = client.generator_efficiency_curve.retrieve("my_generator_efficiency_curve")

        """
        return self._retrieve(external_id, space)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: GeneratorEfficiencyCurveFields | Sequence[GeneratorEfficiencyCurveFields] | None = None,
        group_by: None = None,
        min_ref: float | None = None,
        max_ref: float | None = None,
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
        property: GeneratorEfficiencyCurveFields | Sequence[GeneratorEfficiencyCurveFields] | None = None,
        group_by: GeneratorEfficiencyCurveFields | Sequence[GeneratorEfficiencyCurveFields] = None,
        min_ref: float | None = None,
        max_ref: float | None = None,
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
        property: GeneratorEfficiencyCurveFields | Sequence[GeneratorEfficiencyCurveFields] | None = None,
        group_by: GeneratorEfficiencyCurveFields | Sequence[GeneratorEfficiencyCurveFields] | None = None,
        min_ref: float | None = None,
        max_ref: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across generator efficiency curves

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            min_ref: The minimum value of the ref to filter on.
            max_ref: The maximum value of the ref to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generator efficiency curves to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count generator efficiency curves in space `my_space`:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> result = client.generator_efficiency_curve.aggregate("count", space="my_space")

        """

        filter_ = _create_generator_efficiency_curve_filter(
            self._view_id,
            min_ref,
            max_ref,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _GENERATOREFFICIENCYCURVE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: GeneratorEfficiencyCurveFields,
        interval: float,
        min_ref: float | None = None,
        max_ref: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for generator efficiency curves

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            min_ref: The minimum value of the ref to filter on.
            max_ref: The maximum value of the ref to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generator efficiency curves to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_generator_efficiency_curve_filter(
            self._view_id,
            min_ref,
            max_ref,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _GENERATOREFFICIENCYCURVE_PROPERTIES_BY_FIELD,
            None,
            None,
            limit,
            filter_,
        )

    def list(
        self,
        min_ref: float | None = None,
        max_ref: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> GeneratorEfficiencyCurveList:
        """List/filter generator efficiency curves

        Args:
            min_ref: The minimum value of the ref to filter on.
            max_ref: The maximum value of the ref to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generator efficiency curves to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested generator efficiency curves

        Examples:

            List generator efficiency curves and limit to 5:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> generator_efficiency_curves = client.generator_efficiency_curve.list(limit=5)

        """
        filter_ = _create_generator_efficiency_curve_filter(
            self._view_id,
            min_ref,
            max_ref,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
