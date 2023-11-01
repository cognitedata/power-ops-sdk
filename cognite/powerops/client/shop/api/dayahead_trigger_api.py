import datetime

from cognite.client import CogniteClient
from cognite.client.data_classes import Event, Relationship

from cognite.powerops.client.shop.data_classes.dayahead_trigger import DayaheadTrigger
from cognite.powerops.client.shop.shop_run import SHOPRun
from cognite.powerops.client.shop.shop_run_api import SHOPRunAPI
from cognite.powerops.client.shop.utils import unique_short_str


class DayaheadTriggerAPI:
    def __init__(self, client: CogniteClient, data_set: int, cogshop_version: str):
        self._client = client
        self._data_set_api = data_set
        self._cogshop_version = cogshop_version
        self.shop_run = SHOPRunAPI(client, data_set, cogshop_version)

    def create_trigger_event(self, workflow: DayaheadTrigger, shop_runs: list[SHOPRun]):
        """
        Create a workflow trigger event and link the shop runs to this event.
        (needs method, price scenarios and plant for calculating total bid matrix per plant later on)
        :return: Event used to track the tasks linked to the workflow
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        workflow_event = Event(
            external_id=f"POWEROPS_BID_PROCESS_{workflow.method}_{workflow.num_price_areas}"
            f"_{workflow.price_area}_{unique_short_str(3)}",
            type="POWEROPS_BID_PROCESS_FROM_PRERUNS",
            data_set_id=self._data_set_api,
            start_time=int(now.timestamp()) * 1000,
            end_time=None,
            metadata={
                "bid:market": "Dayahead",
                "bid:main_scenario": workflow.main_scenario,
                "bid:price_scenarios": workflow.price_scenarios,
                "bid:method": workflow.method,
                "bid:price_area": f"price_area_{workflow.price_area}",
                "processed": "true",
            },
        )
        self._client.events.create(workflow_event)
        related_events = []
        for shop_event in shop_runs:
            target_event = shop_event.as_cdf_event(self._data_set_api)
            related_events.append(
                Relationship(
                    external_id=f"{workflow_event.external_id}." f"{target_event.external_id}",
                    source_external_id=workflow_event.external_id,
                    source_type=workflow_event.type,
                    target_external_id=target_event.external_id,
                    target_type=target_event.type,
                    data_set_id=self._data_set_api,
                    labels="relationship_to.shop_run_event",
                )
            )
        self._client.relationships.create(related_events)
        return workflow_event

    def trigger_workflow(self, workflow: DayaheadTrigger) -> dict:
        """
        Creates shop runs for all prerun files referenced in each case.
        Creates a workflow trigger event to link all shop runs to.
        :return: dict with reference to the created workflow event and the shop runs that was created from the workflow
        """
        shop_cases = []
        for case in workflow.cases:
            shop_cases.extend(self.shop_run.trigger_case(case))

        workflow_event = self.create_trigger_event(workflow, shop_cases)
        shop_runs_as_external_ids = [shop_run.as_cdf_event(self._data_set_api).external_id for shop_run in shop_cases]

        return {"workflow_trigger_event": workflow_event.external_id, "shop_run_events": shop_runs_as_external_ids}
