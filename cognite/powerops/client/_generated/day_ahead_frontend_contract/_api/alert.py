from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes import (
    Alert,
    AlertApply,
    AlertApplyList,
    AlertFields,
    AlertList,
    AlertTextFields,
    DomainModelApply,
)
from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes._alert import (
    _ALERT_PROPERTIES_BY_FIELD,
)

from ._core import DEFAULT_LIMIT_READ, Aggregations, TypeAPI


class AlertAPI(TypeAPI[Alert, AlertApply, AlertList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[AlertApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Alert,
            class_apply_type=AlertApply,
            class_list=AlertList,
        )
        self._view_id = view_id
        self._view_by_write_class = view_by_write_class

    def apply(self, alert: AlertApply | Sequence[AlertApply], replace: bool = False) -> dm.InstancesApplyResult:
        """Add or update (upsert) alerts.

        Args:
            alert: Alert or sequence of alerts to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes and edges.

        Examples:

            Create a new alert:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes import AlertApply
                >>> client = DayAheadFrontendContractAPI()
                >>> alert = AlertApply(external_id="my_alert", ...)
                >>> result = client.alert.apply(alert)

        """
        if isinstance(alert, AlertApply):
            instances = alert.to_instances_apply(self._view_by_write_class)
        else:
            instances = AlertApplyList(alert).to_instances_apply(self._view_by_write_class)
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(
        self, external_id: str | Sequence[str], space: str = "dayAheadFrontendContractModel"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more alert.

        Args:
            external_id: External id of the alert to delete.
            space: The space where all the alert are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete alert by id:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> client.alert.delete("my_alert")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Alert:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> AlertList:
        ...

    def retrieve(
        self, external_id: str | Sequence[str], space: str = "dayAheadFrontendContractModel"
    ) -> Alert | AlertList:
        """Retrieve one or more alerts by id(s).

        Args:
            external_id: External id or list of external ids of the alerts.
            space: The space where all the alerts are located.

        Returns:
            The requested alerts.

        Examples:

            Retrieve alert by id:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> alert = client.alert.retrieve("my_alert")

        """
        if isinstance(external_id, str):
            return self._retrieve((space, external_id))
        else:
            return self._retrieve([(space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: AlertTextFields | Sequence[AlertTextFields] | None = None,
        min_time: datetime.datetime | None = None,
        max_time: datetime.datetime | None = None,
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
    ) -> AlertList:
        """Search alerts

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            min_time: The minimum value of the time to filter on.
            max_time: The maximum value of the time to filter on.
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
            limit: Maximum number of alerts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results alerts matching the query.

        Examples:

           Search for 'my_alert' in all text properties:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> alerts = client.alert.search('my_alert')

        """
        filter_ = _create_filter(
            self._view_id,
            min_time,
            max_time,
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
        return self._search(self._view_id, query, _ALERT_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: AlertFields | Sequence[AlertFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: AlertTextFields | Sequence[AlertTextFields] | None = None,
        min_time: datetime.datetime | None = None,
        max_time: datetime.datetime | None = None,
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
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: AlertFields | Sequence[AlertFields] | None = None,
        group_by: AlertFields | Sequence[AlertFields] = None,
        query: str | None = None,
        search_properties: AlertTextFields | Sequence[AlertTextFields] | None = None,
        min_time: datetime.datetime | None = None,
        max_time: datetime.datetime | None = None,
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
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: AlertFields | Sequence[AlertFields] | None = None,
        group_by: AlertFields | Sequence[AlertFields] | None = None,
        query: str | None = None,
        search_property: AlertTextFields | Sequence[AlertTextFields] | None = None,
        min_time: datetime.datetime | None = None,
        max_time: datetime.datetime | None = None,
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
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across alerts

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_time: The minimum value of the time to filter on.
            max_time: The maximum value of the time to filter on.
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
            limit: Maximum number of alerts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count alerts in space `my_space`:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> result = client.alert.aggregate("count", space="my_space")

        """

        filter_ = _create_filter(
            self._view_id,
            min_time,
            max_time,
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
            self._view_id,
            aggregate,
            _ALERT_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: AlertFields,
        interval: float,
        query: str | None = None,
        search_property: AlertTextFields | Sequence[AlertTextFields] | None = None,
        min_time: datetime.datetime | None = None,
        max_time: datetime.datetime | None = None,
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
        """Produces histograms for alerts

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            min_time: The minimum value of the time to filter on.
            max_time: The maximum value of the time to filter on.
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
            limit: Maximum number of alerts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_filter(
            self._view_id,
            min_time,
            max_time,
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
            self._view_id,
            property,
            interval,
            _ALERT_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        min_time: datetime.datetime | None = None,
        max_time: datetime.datetime | None = None,
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
    ) -> AlertList:
        """List/filter alerts

        Args:
            min_time: The minimum value of the time to filter on.
            max_time: The maximum value of the time to filter on.
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
            limit: Maximum number of alerts to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested alerts

        Examples:

            List alerts and limit to 5:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> alerts = client.alert.list(limit=5)

        """
        filter_ = _create_filter(
            self._view_id,
            min_time,
            max_time,
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

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    min_time: datetime.datetime | None = None,
    max_time: datetime.datetime | None = None,
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
) -> dm.Filter | None:
    filters = []
    if min_time or max_time:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("time"),
                gte=min_time.isoformat(timespec="milliseconds") if min_time else None,
                lte=max_time.isoformat(timespec="milliseconds") if max_time else None,
            )
        )
    if title and isinstance(title, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("title"), value=title))
    if title and isinstance(title, list):
        filters.append(dm.filters.In(view_id.as_property_ref("title"), values=title))
    if title_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("title"), value=title_prefix))
    if description and isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if severity and isinstance(severity, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("severity"), value=severity))
    if severity and isinstance(severity, list):
        filters.append(dm.filters.In(view_id.as_property_ref("severity"), values=severity))
    if severity_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("severity"), value=severity_prefix))
    if alert_type and isinstance(alert_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("alertType"), value=alert_type))
    if alert_type and isinstance(alert_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("alertType"), values=alert_type))
    if alert_type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("alertType"), value=alert_type_prefix))
    if min_status_code or max_status_code:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("statusCode"), gte=min_status_code, lte=max_status_code)
        )
    if calculation_run and isinstance(calculation_run, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("calculationRun"), value=calculation_run))
    if calculation_run and isinstance(calculation_run, list):
        filters.append(dm.filters.In(view_id.as_property_ref("calculationRun"), values=calculation_run))
    if calculation_run_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("calculationRun"), value=calculation_run_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
