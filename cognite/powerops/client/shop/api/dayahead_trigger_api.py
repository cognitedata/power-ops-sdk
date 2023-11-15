import datetime

from cognite.client import CogniteClient
from cognite.client.data_classes import Event, Relationship

from cognite.powerops.client.shop.data_classes.dayahead_trigger import DayaheadTrigger, DayaheadTriggerEvent
from cognite.powerops.client.shop.shop_run import SHOPRun
from cognite.powerops.client.shop.shop_run_api import SHOPRunAPI
from cognite.powerops.client.shop.utils import unique_short_str


class DayaheadTriggerAPI:
    def __init__(self, client: CogniteClient, data_set: int, cogshop_version: str):
        self._client = client
        self._data_set_api = data_set
        self.shop_run = SHOPRunAPI(client, data_set, cogshop_version)

    def get_plants_for_case(
        self, case_pre_run_files: list[str], metadata_key: str = "shop_plants", delimiter: str = ","
    ):
        """
        NOT USED ATM
        If case has not been instantiated with plants, fetch the plants from pre run file metadata from cdf. Or update
        this from cogshop?
        """
        pre_run_files_meta = self._client.files.list(external_id=case_pre_run_files, limit=None)
        plants_from_meta = [
            file_meta.metadata.get(metadata_key, "").split(delimiter) for file_meta in pre_run_files_meta
        ]
        return [plant for plants_per_prerun in plants_from_meta for plant in plants_per_prerun]

    def create_trigger_event(self, trigger: DayaheadTrigger, shop_runs: list[SHOPRun]):
        """
        Create a workflow trigger event and link the shop runs to this event.
        (needs method, price scenarios and plant for calculating total bid matrix per plant later on)
        :return: Event used to track the tasks linked to the workflow
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        workflow_event = Event(
            external_id=f"{DayaheadTriggerEvent.external_id_prefix}{trigger.method}_{trigger.num_price_areas}"
            f"_{trigger.price_area}_{unique_short_str(3)}",
            type=DayaheadTriggerEvent.event_type,
            data_set_id=self._data_set_api,
            start_time=int(now.timestamp()) * 1000,
            end_time=None,
            metadata={
                DayaheadTriggerEvent.market: "Dayahead",
                DayaheadTriggerEvent.main_scenario: trigger.main_scenario,
                DayaheadTriggerEvent.price_scenarios: ",".join(trigger.price_scenarios),
                DayaheadTriggerEvent.method: trigger.method,
                DayaheadTriggerEvent.price_area: f"price_area_{trigger.price_area}",
                "processed": "true",
            },
        )
        self._client.events.create(workflow_event)
        related_events = []
        for shop_event in shop_runs:
            target_event = shop_event.as_cdf_event()
            related_events.append(
                Relationship(
                    external_id=f"{workflow_event.external_id}.{target_event.external_id}",
                    source_external_id=workflow_event.external_id,
                    source_type="event",
                    target_external_id=target_event.external_id,
                    target_type="event",
                    data_set_id=self._data_set_api,
                    labels=[DayaheadTriggerEvent.relationship_label_to_shop_run_events],
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
        shop_runs_as_external_ids = [shop_run.as_cdf_event().external_id for shop_run in shop_cases]

        return {"workflow_trigger_event": workflow_event.external_id, "shop_run_events": shop_runs_as_external_ids}
