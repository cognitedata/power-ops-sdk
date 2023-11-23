import datetime
from typing import Optional

from cognite.client.data_classes import Event
from pydantic import BaseModel
from typing_extensions import Self

from cognite.powerops.client.shop.utils import unique_short_str

_SHOP_VERSION_FALLBACK = "15.3.3.2"


class DayaheadFunctionEvent:
    event_type: str = "POWEROPS_FUNCTION_CALL"
    external_id_prefix: str = "POWEROPS_FUNCTION_CALL_"
    function_external_id: str = "function_external_id"
    bid_date: str = "bid:date"
    workflow_event_external_id: str = "workflow_event_external_id"

    @classmethod
    def as_cdf_event(
        cls,
        data_set: int,
        workflow_event_external_id: str,
        bid_date: str,
        event_external_id: str,
        function_name: str,
        additional_metadata: dict,
    ) -> Event:
        return Event(
            external_id=event_external_id,
            type=PartialFunctionEvent.event_type,
            data_set_id=data_set,
            metadata={
                **additional_metadata,
                DayaheadTriggerEvent.bid_date: bid_date,
                DayaheadFunctionEvent.workflow_event_external_id: workflow_event_external_id,
                DayaheadFunctionEvent.function_external_id: function_name,
                "process_type": DayaheadFunctionEvent.event_type,
                "processed": "true",
            },
        )


class PartialFunctionEvent(DayaheadFunctionEvent):
    plant: str = "bid:plant"
    relationship_label_to_trigger_event: str = "relationship_to.bid_process_event"
    method: str = "bid:bid_matrix_generation_method"


class TotalFunctionEvent(DayaheadFunctionEvent):
    portfolio: str = "bid:portfolio"
    bid_process_configuration_name: str = "bid:bid_process_configuration_name"
    relationship_label_to_trigger_event: str = "relationship_to.calculate_total_bid_matrix_event"


class PrerunFileMetadata:
    plants: str = "shop:plants"
    price_scenario: str = "shop:price_scenario"
    _plants_delimiter: str = ","


class DayaheadTriggerEvent:
    event_type: str = "POWEROPS_BID_PROCESS_FROM_PRERUNS"
    external_id_prefix: str = "POWEROPS_BID_PROCESS_"
    market: str = "bid:market"
    main_scenario: str = "bid:main_scenario"
    combine_bid_matrix_tasks: str = "bid:combine_bid_matrix_tasks"
    bid_process_configuration_name: str = "bid:bid_process_configuration_name"
    price_scenarios: str = "bid:price_scenarios"
    method: str = "bid:bid_process_configuration_name"
    price_area: str = "bid:price_area"
    bid_date: str = "bid:date"
    relationship_label_to_shop_run_events: str = "relationship_to.shop_run_event"

    @classmethod
    def as_cdf_event(
        cls,
        data_set: int,
        start_time: datetime,
        bid_date: str,
        price_scenarios: list[str],
        price_area: str,
        method: str,
        main_scenario: str = "",
    ) -> Event:
        return Event(
            external_id=f"{DayaheadTriggerEvent.external_id_prefix}{method}_{len(price_scenarios)}"
            f"_{price_area}_{unique_short_str(3)}",
            type=DayaheadTriggerEvent.event_type,
            data_set_id=data_set,
            start_time=int(start_time.timestamp()) * 1000,
            end_time=None,
            metadata={
                DayaheadTriggerEvent.market: "Dayahead",
                DayaheadTriggerEvent.main_scenario: main_scenario,
                DayaheadTriggerEvent.price_scenarios: ",".join(price_scenarios),
                DayaheadTriggerEvent.method: f"{method}_{len(price_scenarios)}_{price_area}",
                DayaheadTriggerEvent.bid_date: bid_date,
                DayaheadTriggerEvent.bid_process_configuration_name: f"{method}_{len(price_scenarios)}_{price_area}",
                DayaheadTriggerEvent.price_area: f"price_area_{price_area}",
                DayaheadTriggerEvent.combine_bid_matrix_tasks: "true",
                "processed": "true",
            },
        )


class ShopRun(BaseModel):
    """
    Represents a single shop run based on a pre-run file.

    """

    pre_run_external_id: str
    plants: Optional[list[str]] = None
    price_scenario: Optional[str] = None

    @classmethod
    def load_from_metadata(cls, file_external_id: str, file_metadata: dict) -> Self:
        plants_as_string = file_metadata.get(PrerunFileMetadata.plants)
        plants = [plant.lstrip() for plant in plants_as_string.split(PrerunFileMetadata._plants_delimiter)]
        return cls(
            pre_run_external_id=file_external_id,
            plants=plants,
            price_scenario=file_metadata.get(PrerunFileMetadata.price_scenario),
        )


class Case(BaseModel):
    """
    Case definition based on a set of prerun files to run for certain plants that are used in the set of prerun files
    """

    case_name: str  # e.g. Glomma
    pre_runs: list[ShopRun]
    commands_file: Optional[str] = None
    pre_runs_external_id_prefix: Optional[str] = None


class DayaheadTrigger(BaseModel):
    """
    Contains the blueprint of a Dayahead workflow definition.
    This object is used together with the DayaheadTriggerAPI to trigger a workflow from an instance of this class.
    """

    price_scenarios: list[str]
    main_scenario: str = ""
    shop_version: Optional[str] = _SHOP_VERSION_FALLBACK
    price_area: str
    method: str
    cases: list[Case]
    _plants_per_workflow: list[str]

    @property
    def plants_per_workflow(self):
        return self._plants_per_workflow

    @plants_per_workflow.setter
    def plants_per_workflow(self, value):
        self._plants_per_workflow = value
