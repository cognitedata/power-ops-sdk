import datetime
from collections import defaultdict

from cognite.client import CogniteClient
from cognite.client.data_classes import Event, Relationship

from cognite.powerops.client.shop.data_classes.dayahead_trigger import (
    DayaheadTrigger,
    DayaheadTriggerEvent,
    PartialFunctionEvent,
    TotalFunctionEvent,
)
from cognite.powerops.client.shop.shop_run_api import SHOPRunAPI
from cognite.powerops.client.shop.utils import unique_short_str


class DayaheadTriggerAPI:
    def __init__(self, client: CogniteClient, data_set: int, cogshop_version: str = ""):
        self._client = client
        self._data_set_api = data_set
        self.shop_run = SHOPRunAPI(client, data_set, cogshop_version=cogshop_version)

    def _create_function_events(
        self, workflow: DayaheadTrigger, workflow_event_external_id: str, bid_date: str
    ) -> dict:
        """
        Create the partial and total bid matrix calculation events that are needed for the partial and total bid
        matrix calculations in the Cognite Functions.
        """
        events_and_relationships: defaultdict = defaultdict(list, {k: [] for k in ("events", "relationships")})
        short_id = unique_short_str(3)

        for plant in workflow.plants_per_workflow:
            partial_matrix_event_external_id = f"{PartialFunctionEvent.external_id_prefix}{short_id}_{plant}"
            metadata = {PartialFunctionEvent.plant: plant, PartialFunctionEvent.method: workflow.method}
            events_and_relationships["events"].append(
                PartialFunctionEvent.as_cdf_event(
                    data_set=self._data_set_api,
                    bid_date=bid_date,
                    workflow_event_external_id=workflow_event_external_id,
                    event_external_id=partial_matrix_event_external_id,
                    function_name="generate_bid_matrix",
                    additional_metadata=metadata,
                )
            )
            events_and_relationships["relationships"].append(
                Relationship(
                    external_id=f"{workflow_event_external_id}.{partial_matrix_event_external_id}",
                    source_external_id=workflow_event_external_id,
                    source_type="event",
                    target_external_id=partial_matrix_event_external_id,
                    target_type="event",
                    data_set_id=self._data_set_api,
                    labels=[PartialFunctionEvent.relationship_label_to_trigger_event],
                )
            )
        total_event_external_id = f"{TotalFunctionEvent.external_id_prefix}{short_id}"
        events_and_relationships["events"].append(
            TotalFunctionEvent.as_cdf_event(
                data_set=self._data_set_api,
                bid_date=bid_date,
                workflow_event_external_id=workflow_event_external_id,
                event_external_id=total_event_external_id,
                function_name="calculate_total_bid_matrix",
                additional_metadata={
                    TotalFunctionEvent.portfolio: f"price_area_{workflow.price_area}",
                    TotalFunctionEvent.bid_process_configuration_name: f"{workflow.method}_"
                    f"{len(workflow.price_scenarios)}_"
                    f"{workflow.price_area}",
                },
            )
        )
        events_and_relationships["relationships"].append(
            Relationship(
                external_id=f"{workflow_event_external_id}.{total_event_external_id}",
                source_external_id=workflow_event_external_id,
                source_type="event",
                target_external_id=total_event_external_id,
                target_type="event",
                data_set_id=self._data_set_api,
                labels=[TotalFunctionEvent.relationship_label_to_trigger_event],
            )
        )

        return events_and_relationships

    def _create_trigger_event(self, workflow: DayaheadTrigger, start_time: datetime, bid_date: str) -> Event:
        """
        Create a workflow trigger event and link the shop runs to this event.
        (needs method, price scenarios and plant for calculating total bid matrix per plant later on)
        Returns:
            Event used to track the tasks linked to the workflow
        """
        return DayaheadTriggerEvent.as_cdf_event(
            data_set=self._data_set_api,
            start_time=start_time,
            bid_date=bid_date,
            price_area=workflow.price_area,
            price_scenarios=workflow.price_scenarios,
            method=workflow.method,
        )

    def _create_and_wire_workflow_events(
        self, workflow: DayaheadTrigger, shop_runs_as_cdf_events: list[Event], start_time: datetime, bid_date: str
    ) -> dict[str, Event | list[Event]]:
        workflow_event = self._create_trigger_event(workflow, start_time, bid_date)
        events_and_relationships = self._create_function_events(
            workflow=workflow, workflow_event_external_id=workflow_event.external_id, bid_date=bid_date
        )
        self._client.events.create([workflow_event, *events_and_relationships["events"]])

        shop_relationships = [
            Relationship(
                external_id=f"{workflow_event.external_id}.{shop_event.external_id}",
                source_external_id=workflow_event.external_id,
                source_type="event",
                target_external_id=shop_event.external_id,
                target_type="event",
                data_set_id=self._data_set_api,
                labels=[DayaheadTriggerEvent.relationship_label_to_shop_run_events],
            )
            for shop_event in shop_runs_as_cdf_events
        ]
        self._client.relationships.create([*events_and_relationships["relationships"], *shop_relationships])

        return {
            "workflow_trigger_event": workflow_event,
            "bid_matrix_function_events": events_and_relationships["events"],
            "shop_run_events": shop_runs_as_cdf_events,
        }

    def trigger_workflow(self, workflow: DayaheadTrigger) -> dict[str, Event | list[Event]]:
        """
        Creates shop runs for all prerun files referenced in each case.
        Creates a workflow trigger event that gets linked to all shop runs.
        Creates Events for Partial and Total bid matrix calculation that are also linked to the trigger event.
        Args:
            workflow: DayaheadTrigger object to trigger a dayahead workflow from
        Returns:
            dict with reference to the created workflow event and the shop runs that was created from the workflow
        """
        start_time = datetime.datetime.now(datetime.timezone.utc)
        bid_date = (start_time + datetime.timedelta(days=1)).strftime("%Y-%m-%d")

        shop_runs = []
        plants_per_workflow = []
        for case in workflow.cases:
            plants_per_case, shop_runs_per_case = self.shop_run.trigger_case(case, workflow.shop_version)
            plants_per_workflow.extend(plants_per_case)
            shop_runs.extend(shop_runs_per_case)

        workflow.plants_per_workflow = list(set(plants_per_workflow))
        return self._create_and_wire_workflow_events(
            workflow,
            shop_runs_as_cdf_events=[shop_run.as_cdf_event() for shop_run in shop_runs],
            start_time=start_time,
            bid_date=bid_date,
        )
