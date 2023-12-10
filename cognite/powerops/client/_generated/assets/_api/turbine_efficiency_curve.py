from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.assets.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.assets.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    TurbineEfficiencyCurve,
    TurbineEfficiencyCurveApply,
    TurbineEfficiencyCurveFields,
    TurbineEfficiencyCurveList,
    TurbineEfficiencyCurveApplyList,
)
from cognite.powerops.client._generated.assets.data_classes._turbine_efficiency_curve import (
    _TURBINEEFFICIENCYCURVE_PROPERTIES_BY_FIELD,
    _create_turbine_efficiency_curve_filter,
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
from .turbine_efficiency_curve_query import TurbineEfficiencyCurveQueryAPI


class TurbineEfficiencyCurveAPI(
    NodeAPI[TurbineEfficiencyCurve, TurbineEfficiencyCurveApply, TurbineEfficiencyCurveList]
):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[TurbineEfficiencyCurveApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=TurbineEfficiencyCurve,
            class_apply_type=TurbineEfficiencyCurveApply,
            class_list=TurbineEfficiencyCurveList,
            class_apply_list=TurbineEfficiencyCurveApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        min_head: float | None = None,
        max_head: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> TurbineEfficiencyCurveQueryAPI[TurbineEfficiencyCurveList]:
        """Query starting at turbine efficiency curves.

        Args:
            min_head: The minimum value of the head to filter on.
            max_head: The maximum value of the head to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of turbine efficiency curves to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for turbine efficiency curves.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_turbine_efficiency_curve_filter(
            self._view_id,
            min_head,
            max_head,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(TurbineEfficiencyCurveList)
        return TurbineEfficiencyCurveQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self,
        turbine_efficiency_curve: TurbineEfficiencyCurveApply | Sequence[TurbineEfficiencyCurveApply],
        replace: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) turbine efficiency curves.

        Args:
            turbine_efficiency_curve: Turbine efficiency curve or sequence of turbine efficiency curves to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new turbine_efficiency_curve:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> from cognite.powerops.client._generated.assets.data_classes import TurbineEfficiencyCurveApply
                >>> client = PowerAssetAPI()
                >>> turbine_efficiency_curve = TurbineEfficiencyCurveApply(external_id="my_turbine_efficiency_curve", ...)
                >>> result = client.turbine_efficiency_curve.apply(turbine_efficiency_curve)

        """
        return self._apply(turbine_efficiency_curve, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more turbine efficiency curve.

        Args:
            external_id: External id of the turbine efficiency curve to delete.
            space: The space where all the turbine efficiency curve are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete turbine_efficiency_curve by id:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> client.turbine_efficiency_curve.delete("my_turbine_efficiency_curve")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> TurbineEfficiencyCurve | None:
        ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> TurbineEfficiencyCurveList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> TurbineEfficiencyCurve | TurbineEfficiencyCurveList | None:
        """Retrieve one or more turbine efficiency curves by id(s).

        Args:
            external_id: External id or list of external ids of the turbine efficiency curves.
            space: The space where all the turbine efficiency curves are located.

        Returns:
            The requested turbine efficiency curves.

        Examples:

            Retrieve turbine_efficiency_curve by id:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> turbine_efficiency_curve = client.turbine_efficiency_curve.retrieve("my_turbine_efficiency_curve")

        """
        return self._retrieve(external_id, space)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: TurbineEfficiencyCurveFields | Sequence[TurbineEfficiencyCurveFields] | None = None,
        group_by: None = None,
        min_head: float | None = None,
        max_head: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: TurbineEfficiencyCurveFields | Sequence[TurbineEfficiencyCurveFields] | None = None,
        group_by: TurbineEfficiencyCurveFields | Sequence[TurbineEfficiencyCurveFields] = None,
        min_head: float | None = None,
        max_head: float | None = None,
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
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: TurbineEfficiencyCurveFields | Sequence[TurbineEfficiencyCurveFields] | None = None,
        group_by: TurbineEfficiencyCurveFields | Sequence[TurbineEfficiencyCurveFields] | None = None,
        min_head: float | None = None,
        max_head: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across turbine efficiency curves

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            min_head: The minimum value of the head to filter on.
            max_head: The maximum value of the head to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of turbine efficiency curves to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count turbine efficiency curves in space `my_space`:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> result = client.turbine_efficiency_curve.aggregate("count", space="my_space")

        """

        filter_ = _create_turbine_efficiency_curve_filter(
            self._view_id,
            min_head,
            max_head,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _TURBINEEFFICIENCYCURVE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: TurbineEfficiencyCurveFields,
        interval: float,
        min_head: float | None = None,
        max_head: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for turbine efficiency curves

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            min_head: The minimum value of the head to filter on.
            max_head: The maximum value of the head to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of turbine efficiency curves to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_turbine_efficiency_curve_filter(
            self._view_id,
            min_head,
            max_head,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _TURBINEEFFICIENCYCURVE_PROPERTIES_BY_FIELD,
            None,
            None,
            limit,
            filter_,
        )

    def list(
        self,
        min_head: float | None = None,
        max_head: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TurbineEfficiencyCurveList:
        """List/filter turbine efficiency curves

        Args:
            min_head: The minimum value of the head to filter on.
            max_head: The maximum value of the head to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of turbine efficiency curves to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested turbine efficiency curves

        Examples:

            List turbine efficiency curves and limit to 5:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> turbine_efficiency_curves = client.turbine_efficiency_curve.list(limit=5)

        """
        filter_ = _create_turbine_efficiency_curve_filter(
            self._view_id,
            min_head,
            max_head,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
