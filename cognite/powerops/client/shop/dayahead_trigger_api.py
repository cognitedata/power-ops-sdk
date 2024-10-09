from __future__ import annotations

from collections import defaultdict

from cognite.client import CogniteClient
from cognite.client.data_classes import Event, Relationship

from cognite.powerops.client.shop.data_classes.dayahead_trigger import (
    DayaheadTrigger,
    DayaheadTriggerEvent,
    DayaheadWorkflowRun,
    PartialFunctionEvent,
    TotalFunctionEvent,
)
from cognite.powerops.client.shop.shop_run_api import SHOPRunAPI
from cognite.powerops.utils.deprecation import deprecated_class
from cognite.powerops.utils.identifiers import unique_short_str


# ! Marked for deprecation, will be removed
@deprecated_class
class DayaheadTriggerAPI:
    def __init__(self, client: CogniteClient, data_set: int):
        self._client = client
        self._data_set_api = data_set
        self.shop_run = SHOPRunAPI(client, data_set)

    def _create_partial_function_events(
        self,
        workflow: DayaheadTrigger,
        workflow_event_external_id: str,
        suffix_run_id: str,
        plants_per_workflow: list[str],
    ) -> dict:
        """
        Create the partial and total bid matrix calculation events that are needed for the partial and total bid
        matrix calculations in the Cognite Functions.
        """
        events_and_relationships: defaultdict = defaultdict(list, {k: [] for k in ("events", "relationships")})

        for plant in plants_per_workflow:
            partial_matrix_event_external_id = f"{PartialFunctionEvent.external_id_prefix}{suffix_run_id}_{plant}"
            metadata = {
                PartialFunctionEvent.plant: plant,
                PartialFunctionEvent.method: workflow.method,
            }
            if workflow.plant_names_override:
                metadata[PartialFunctionEvent.plant_name_override] = workflow.plant_names_override[plant]

            events_and_relationships["events"].append(
                PartialFunctionEvent.as_cdf_event(
                    data_set=self._data_set_api,
                    bid_date=workflow.bid_time_frame.bid_date,
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
        return events_and_relationships

    def _create_total_matrix_event(
        self,
        workflow: DayaheadTrigger,
        workflow_event_external_id: str,
        suffix_run_id: str,
    ) -> tuple[Event, Relationship]:
        total_event_external_id = f"{TotalFunctionEvent.external_id_prefix}{suffix_run_id}"
        total_event = TotalFunctionEvent.as_cdf_event(
            data_set=self._data_set_api,
            bid_date=workflow.bid_time_frame.bid_date,
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
        total_event_relationship = Relationship(
            external_id=f"{workflow_event_external_id}.{total_event_external_id}",
            source_external_id=workflow_event_external_id,
            source_type="event",
            target_external_id=total_event_external_id,
            target_type="event",
            data_set_id=self._data_set_api,
            labels=[TotalFunctionEvent.relationship_label_to_trigger_event],
        )

        return (total_event, total_event_relationship)

    def _create_trigger_event(self, workflow: DayaheadTrigger, suffix_run_id: str) -> Event:
        """
        Create a workflow trigger event and link the shop runs to this event.
        (needs method, price scenarios and plant for calculating total bid matrix per plant later on)
        Returns:
            Event used to track the tasks linked to the workflow
        """
        return DayaheadTriggerEvent.as_cdf_event(
            data_set=self._data_set_api,
            event_external_id=f"{DayaheadTriggerEvent.external_id_prefix}{workflow.bid_configuration_name}_{suffix_run_id}",
            start_time=workflow.bid_time_frame.start_time_string,
            end_time=workflow.bid_time_frame.end_time_string,
            bid_date=workflow.bid_time_frame.bid_date,
            price_area=workflow.price_area,
            price_scenarios=workflow.price_scenarios,
            bid_configuration_name=workflow.bid_configuration_name,
            market_configuration_nordpool_dayahead=workflow.dayahead_configuration_external_id,
        )

    def _create_and_wire_workflow_events(
        self,
        workflow: DayaheadTrigger,
        shop_runs_as_cdf_events: list[Event],
        plants_per_workflow: list[str],
    ) -> DayaheadWorkflowRun:
        suffix_run_id = unique_short_str(3)

        workflow_event = self._create_trigger_event(workflow, suffix_run_id)
        partial_events_and_relationships = self._create_partial_function_events(
            workflow=workflow,
            plants_per_workflow=plants_per_workflow,
            workflow_event_external_id=workflow_event.external_id,
            suffix_run_id=suffix_run_id,
        )
        total_event, total_event_relationship = self._create_total_matrix_event(
            workflow=workflow,
            workflow_event_external_id=workflow_event.external_id,
            suffix_run_id=suffix_run_id,
        )
        self._client.events.create([workflow_event, *partial_events_and_relationships["events"], total_event])
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
        self._client.relationships.create(
            [
                *partial_events_and_relationships["relationships"],
                total_event_relationship,
                *shop_relationships,
            ]
        )

        return DayaheadWorkflowRun(
            workflow_trigger_event=workflow_event,
            total_bid_event=total_event,
            partial_bid_events=partial_events_and_relationships["events"],
            shop_run_events=shop_runs_as_cdf_events,
        )

    def trigger_workflow(self, workflow: DayaheadTrigger) -> DayaheadWorkflowRun:
        """
        Creates shop runs for all prerun files referenced in each case.
        Creates a workflow trigger event that gets linked to all shop runs.
        Creates Events for Partial and Total bid matrix calculation that are also linked to the trigger event.

        Args:
            workflow: DayaheadTrigger object to trigger a dayahead workflow from
        Returns:
            DayaheadWorkflowRun that holds the SHOP run events and the bid calculation events (see DayaheadWorkflowRun
            for more info)
        """

        shop_runs = []
        plants_per_workflow = []
        for case in workflow.cases:
            plants_per_case, shop_runs_per_case = self.shop_run.trigger_case(case, workflow.shop_version)
            plants_per_workflow.extend(plants_per_case)
            shop_runs.extend(shop_runs_per_case)

        return self._create_and_wire_workflow_events(
            workflow,
            plants_per_workflow=plants_per_workflow,
            shop_runs_as_cdf_events=[shop_run.as_cdf_event() for shop_run in shop_runs],
        )
