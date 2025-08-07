from __future__ import annotations

import datetime
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
from cognite.powerops.client._generated.v1.data_classes._shop_penalty_report import (
    ShopPenaltyReportQuery,
    _SHOPPENALTYREPORT_PROPERTIES_BY_FIELD,
    _create_shop_penalty_report_filter,
)
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    ShopPenaltyReport,
    ShopPenaltyReportWrite,
    ShopPenaltyReportFields,
    ShopPenaltyReportList,
    ShopPenaltyReportWriteList,
    ShopPenaltyReportTextFields,
)


class ShopPenaltyReportAPI(NodeAPI[ShopPenaltyReport, ShopPenaltyReportWrite, ShopPenaltyReportList, ShopPenaltyReportWriteList]):
    _view_id = dm.ViewId("power_ops_core", "ShopPenaltyReport", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _SHOPPENALTYREPORT_PROPERTIES_BY_FIELD
    _class_type = ShopPenaltyReport
    _class_list = ShopPenaltyReportList
    _class_write_list = ShopPenaltyReportWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)


    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> ShopPenaltyReport | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> ShopPenaltyReportList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> ShopPenaltyReport | ShopPenaltyReportList | None:
        """Retrieve one or more shop penalty reports by id(s).

        Args:
            external_id: External id or list of external ids of the shop penalty reports.
            space: The space where all the shop penalty reports are located.

        Returns:
            The requested shop penalty reports.

        Examples:

            Retrieve shop_penalty_report by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_penalty_report = client.shop_penalty_report.retrieve(
                ...     "my_shop_penalty_report"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
        )

    def search(
        self,
        query: str,
        properties: ShopPenaltyReportTextFields | SequenceNotStr[ShopPenaltyReportTextFields] | None = None,
        min_time: datetime.datetime | None = None,
        max_time: datetime.datetime | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        severity: str | list[str] | None = None,
        severity_prefix: str | None = None,
        alert_type: str | list[str] | None = None,
        alert_type_prefix: str | None = None,
        min_status_code: int | None = None,
        max_status_code: int | None = None,
        calculation_run: str | list[str] | None = None,
        calculation_run_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ShopPenaltyReportFields | SequenceNotStr[ShopPenaltyReportFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ShopPenaltyReportList:
        """Search shop penalty reports

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            min_time: The minimum value of the time to filter on.
            max_time: The maximum value of the time to filter on.
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            title: The title to filter on.
            title_prefix: The prefix of the title to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            severity: The severity to filter on.
            severity_prefix: The prefix of the severity to filter on.
            alert_type: The alert type to filter on.
            alert_type_prefix: The prefix of the alert type to filter on.
            min_status_code: The minimum value of the status code to filter on.
            max_status_code: The maximum value of the status code to filter on.
            calculation_run: The calculation run to filter on.
            calculation_run_prefix: The prefix of the calculation run to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop penalty reports to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results shop penalty reports matching the query.

        Examples:

           Search for 'my_shop_penalty_report' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_penalty_reports = client.shop_penalty_report.search(
                ...     'my_shop_penalty_report'
                ... )

        """
        filter_ = _create_shop_penalty_report_filter(
            self._view_id,
            min_time,
            max_time,
            workflow_execution_id,
            workflow_execution_id_prefix,
            title,
            title_prefix,
            description,
            description_prefix,
            severity,
            severity_prefix,
            alert_type,
            alert_type_prefix,
            min_status_code,
            max_status_code,
            calculation_run,
            calculation_run_prefix,
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
        property: ShopPenaltyReportFields | SequenceNotStr[ShopPenaltyReportFields] | None = None,
        query: str | None = None,
        search_property: ShopPenaltyReportTextFields | SequenceNotStr[ShopPenaltyReportTextFields] | None = None,
        min_time: datetime.datetime | None = None,
        max_time: datetime.datetime | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        severity: str | list[str] | None = None,
        severity_prefix: str | None = None,
        alert_type: str | list[str] | None = None,
        alert_type_prefix: str | None = None,
        min_status_code: int | None = None,
        max_status_code: int | None = None,
        calculation_run: str | list[str] | None = None,
        calculation_run_prefix: str | None = None,
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
        property: ShopPenaltyReportFields | SequenceNotStr[ShopPenaltyReportFields] | None = None,
        query: str | None = None,
        search_property: ShopPenaltyReportTextFields | SequenceNotStr[ShopPenaltyReportTextFields] | None = None,
        min_time: datetime.datetime | None = None,
        max_time: datetime.datetime | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        severity: str | list[str] | None = None,
        severity_prefix: str | None = None,
        alert_type: str | list[str] | None = None,
        alert_type_prefix: str | None = None,
        min_status_code: int | None = None,
        max_status_code: int | None = None,
        calculation_run: str | list[str] | None = None,
        calculation_run_prefix: str | None = None,
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
        group_by: ShopPenaltyReportFields | SequenceNotStr[ShopPenaltyReportFields],
        property: ShopPenaltyReportFields | SequenceNotStr[ShopPenaltyReportFields] | None = None,
        query: str | None = None,
        search_property: ShopPenaltyReportTextFields | SequenceNotStr[ShopPenaltyReportTextFields] | None = None,
        min_time: datetime.datetime | None = None,
        max_time: datetime.datetime | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        severity: str | list[str] | None = None,
        severity_prefix: str | None = None,
        alert_type: str | list[str] | None = None,
        alert_type_prefix: str | None = None,
        min_status_code: int | None = None,
        max_status_code: int | None = None,
        calculation_run: str | list[str] | None = None,
        calculation_run_prefix: str | None = None,
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
        group_by: ShopPenaltyReportFields | SequenceNotStr[ShopPenaltyReportFields] | None = None,
        property: ShopPenaltyReportFields | SequenceNotStr[ShopPenaltyReportFields] | None = None,
        query: str | None = None,
        search_property: ShopPenaltyReportTextFields | SequenceNotStr[ShopPenaltyReportTextFields] | None = None,
        min_time: datetime.datetime | None = None,
        max_time: datetime.datetime | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        severity: str | list[str] | None = None,
        severity_prefix: str | None = None,
        alert_type: str | list[str] | None = None,
        alert_type_prefix: str | None = None,
        min_status_code: int | None = None,
        max_status_code: int | None = None,
        calculation_run: str | list[str] | None = None,
        calculation_run_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across shop penalty reports

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_time: The minimum value of the time to filter on.
            max_time: The maximum value of the time to filter on.
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            title: The title to filter on.
            title_prefix: The prefix of the title to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            severity: The severity to filter on.
            severity_prefix: The prefix of the severity to filter on.
            alert_type: The alert type to filter on.
            alert_type_prefix: The prefix of the alert type to filter on.
            min_status_code: The minimum value of the status code to filter on.
            max_status_code: The maximum value of the status code to filter on.
            calculation_run: The calculation run to filter on.
            calculation_run_prefix: The prefix of the calculation run to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop penalty reports to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count shop penalty reports in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.shop_penalty_report.aggregate("count", space="my_space")

        """

        filter_ = _create_shop_penalty_report_filter(
            self._view_id,
            min_time,
            max_time,
            workflow_execution_id,
            workflow_execution_id_prefix,
            title,
            title_prefix,
            description,
            description_prefix,
            severity,
            severity_prefix,
            alert_type,
            alert_type_prefix,
            min_status_code,
            max_status_code,
            calculation_run,
            calculation_run_prefix,
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
        property: ShopPenaltyReportFields,
        interval: float,
        query: str | None = None,
        search_property: ShopPenaltyReportTextFields | SequenceNotStr[ShopPenaltyReportTextFields] | None = None,
        min_time: datetime.datetime | None = None,
        max_time: datetime.datetime | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        severity: str | list[str] | None = None,
        severity_prefix: str | None = None,
        alert_type: str | list[str] | None = None,
        alert_type_prefix: str | None = None,
        min_status_code: int | None = None,
        max_status_code: int | None = None,
        calculation_run: str | list[str] | None = None,
        calculation_run_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for shop penalty reports

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_time: The minimum value of the time to filter on.
            max_time: The maximum value of the time to filter on.
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            title: The title to filter on.
            title_prefix: The prefix of the title to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            severity: The severity to filter on.
            severity_prefix: The prefix of the severity to filter on.
            alert_type: The alert type to filter on.
            alert_type_prefix: The prefix of the alert type to filter on.
            min_status_code: The minimum value of the status code to filter on.
            max_status_code: The maximum value of the status code to filter on.
            calculation_run: The calculation run to filter on.
            calculation_run_prefix: The prefix of the calculation run to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop penalty reports to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_shop_penalty_report_filter(
            self._view_id,
            min_time,
            max_time,
            workflow_execution_id,
            workflow_execution_id_prefix,
            title,
            title_prefix,
            description,
            description_prefix,
            severity,
            severity_prefix,
            alert_type,
            alert_type_prefix,
            min_status_code,
            max_status_code,
            calculation_run,
            calculation_run_prefix,
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

    def select(self) -> ShopPenaltyReportQuery:
        """Start selecting from shop penalty reports."""
        return ShopPenaltyReportQuery(self._client)

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
        min_time: datetime.datetime | None = None,
        max_time: datetime.datetime | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        severity: str | list[str] | None = None,
        severity_prefix: str | None = None,
        alert_type: str | list[str] | None = None,
        alert_type_prefix: str | None = None,
        min_status_code: int | None = None,
        max_status_code: int | None = None,
        calculation_run: str | list[str] | None = None,
        calculation_run_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[ShopPenaltyReportList]:
        """Iterate over shop penalty reports

        Args:
            chunk_size: The number of shop penalty reports to return in each iteration. Defaults to 100.
            min_time: The minimum value of the time to filter on.
            max_time: The maximum value of the time to filter on.
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            title: The title to filter on.
            title_prefix: The prefix of the title to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            severity: The severity to filter on.
            severity_prefix: The prefix of the severity to filter on.
            alert_type: The alert type to filter on.
            alert_type_prefix: The prefix of the alert type to filter on.
            min_status_code: The minimum value of the status code to filter on.
            max_status_code: The maximum value of the status code to filter on.
            calculation_run: The calculation run to filter on.
            calculation_run_prefix: The prefix of the calculation run to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of shop penalty reports to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of shop penalty reports

        Examples:

            Iterate shop penalty reports in chunks of 100 up to 2000 items:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for shop_penalty_reports in client.shop_penalty_report.iterate(chunk_size=100, limit=2000):
                ...     for shop_penalty_report in shop_penalty_reports:
                ...         print(shop_penalty_report.external_id)

            Iterate shop penalty reports in chunks of 100 sorted by external_id in descending order:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for shop_penalty_reports in client.shop_penalty_report.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for shop_penalty_report in shop_penalty_reports:
                ...         print(shop_penalty_report.external_id)

            Iterate shop penalty reports in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for first_iteration in client.shop_penalty_report.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for shop_penalty_reports in client.shop_penalty_report.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for shop_penalty_report in shop_penalty_reports:
                ...         print(shop_penalty_report.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_shop_penalty_report_filter(
            self._view_id,
            min_time,
            max_time,
            workflow_execution_id,
            workflow_execution_id_prefix,
            title,
            title_prefix,
            description,
            description_prefix,
            severity,
            severity_prefix,
            alert_type,
            alert_type_prefix,
            min_status_code,
            max_status_code,
            calculation_run,
            calculation_run_prefix,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, "skip", cursors=cursors)

    def list(
        self,
        min_time: datetime.datetime | None = None,
        max_time: datetime.datetime | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        title: str | list[str] | None = None,
        title_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        severity: str | list[str] | None = None,
        severity_prefix: str | None = None,
        alert_type: str | list[str] | None = None,
        alert_type_prefix: str | None = None,
        min_status_code: int | None = None,
        max_status_code: int | None = None,
        calculation_run: str | list[str] | None = None,
        calculation_run_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ShopPenaltyReportFields | Sequence[ShopPenaltyReportFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ShopPenaltyReportList:
        """List/filter shop penalty reports

        Args:
            min_time: The minimum value of the time to filter on.
            max_time: The maximum value of the time to filter on.
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            title: The title to filter on.
            title_prefix: The prefix of the title to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            severity: The severity to filter on.
            severity_prefix: The prefix of the severity to filter on.
            alert_type: The alert type to filter on.
            alert_type_prefix: The prefix of the alert type to filter on.
            min_status_code: The minimum value of the status code to filter on.
            max_status_code: The maximum value of the status code to filter on.
            calculation_run: The calculation run to filter on.
            calculation_run_prefix: The prefix of the calculation run to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop penalty reports to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested shop penalty reports

        Examples:

            List shop penalty reports and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_penalty_reports = client.shop_penalty_report.list(limit=5)

        """
        filter_ = _create_shop_penalty_report_filter(
            self._view_id,
            min_time,
            max_time,
            workflow_execution_id,
            workflow_execution_id_prefix,
            title,
            title_prefix,
            description,
            description_prefix,
            severity,
            severity_prefix,
            alert_type,
            alert_type_prefix,
            min_status_code,
            max_status_code,
            calculation_run,
            calculation_run_prefix,
            external_id_prefix,
            space,
            filter,
        )
        sort_input =  self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        return self._list(limit=limit,  filter=filter_, sort=sort_input)
