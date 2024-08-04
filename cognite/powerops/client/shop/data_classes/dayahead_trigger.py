from typing import Literal, Optional

import arrow
from cognite.client.data_classes import Event, FileMetadata
from pydantic import BaseModel, ConfigDict, field_validator
from typing_extensions import Self

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
                cls.bid_date: bid_date,
                cls.workflow_event_external_id: workflow_event_external_id,
                cls.function_external_id: function_name,
                "process_type": DayaheadFunctionEvent.event_type,
                "processed": "true",
            },
        )


class PartialFunctionEvent(DayaheadFunctionEvent):
    plant: str = "bid:plant"
    method: str = "bid:bid_matrix_generation_method"
    relationship_label_to_trigger_event: str = "relationship_to.bid_process_event"
    # This is a temporary hack that is introduced due
    # to the potential inconsistency between plant names in
    # SHOP files and the plant asset names in CDF
    plant_name_override: str = "bid:plant_name_override"


class TotalFunctionEvent(DayaheadFunctionEvent):
    portfolio: str = "bid:portfolio"
    bid_process_configuration_name: str = "bid:bid_process_configuration_name"
    relationship_label_to_trigger_event: str = "relationship_to.calculate_total_bid_matrix_event"


class DayaheadTriggerEvent:
    event_type: str = "POWEROPS_BID_PROCESS_FROM_PRERUNS"
    external_id_prefix: str = "POWEROPS_BID_PROCESS_"
    market: str = "bid:market"
    main_scenario: str = "bid:main_scenario"
    market_configuration_nordpool_dayahead: str = "bid:market_config_external_id"
    combine_bid_matrix_tasks: str = "bid:combine_bid_matrix_tasks"
    bid_process_configuration_name: str = "bid:bid_process_configuration_name"
    bid_matrix_generator_config_external_id: str = "bid:bid_matrix_generator_config_external_id"
    price_scenarios: str = "bid:price_scenarios"
    method: str = "bid:bid_process_configuration_name"
    price_area: str = "bid:price_area"
    bid_date: str = "bid:date"
    start_time: str = "shop:starttime"
    end_time: str = "shop:endtime"
    relationship_label_to_shop_run_events: str = "relationship_to.shop_run_event"

    @classmethod
    def as_cdf_event(
        cls,
        event_external_id: str,
        data_set: int,
        start_time: str,
        end_time: str,
        bid_date: str,
        price_scenarios: list[str],
        price_area: str,
        market_configuration_nordpool_dayahead: str,
        bid_configuration_name,
        main_scenario: str = "",
    ) -> Event:
        return Event(
            external_id=event_external_id,
            type=DayaheadTriggerEvent.event_type,
            data_set_id=data_set,
            metadata={
                cls.market: "Dayahead",
                cls.main_scenario: main_scenario,
                cls.price_scenarios: ",".join(price_scenarios),
                cls.bid_date: bid_date,
                cls.start_time: start_time,
                cls.end_time: end_time,
                cls.market_configuration_nordpool_dayahead: market_configuration_nordpool_dayahead,
                cls.bid_process_configuration_name: bid_configuration_name,
                cls.bid_matrix_generator_config_external_id: f"POWEROPS_bid_matrix_generator_config_"
                f"{bid_configuration_name}",
                cls.price_area: f"price_area_{price_area}",
                cls.combine_bid_matrix_tasks: "true",
                "processed": "true",
            },
        )


class PrerunFileMetadata:
    plants: str = "shop:plants"
    price_scenario: str = "shop:price_scenario"
    _plants_delimiter: str = ","


class SHOPPreRunFile(BaseModel):
    """
    Represents a single shop run based on a pre-run file.
    """

    pre_run_external_id: str
    plants: list[str]
    price_scenario: str

    @classmethod
    def load_from_metadata(cls, file_metadata: FileMetadata) -> Self:
        plants_as_string = file_metadata.metadata.get(PrerunFileMetadata.plants)
        plants = [plant.lstrip() for plant in plants_as_string.split(PrerunFileMetadata._plants_delimiter)]
        return cls(
            pre_run_external_id=file_metadata.external_id,
            plants=plants,
            price_scenario=file_metadata.metadata.get(PrerunFileMetadata.price_scenario),
        )


class Case(BaseModel):
    """
    Case definition based on a set of prerun files to run for certain plants that are used in the set of prerun files

    """

    case_name: str  # e.g. Glomma
    pre_run_file_external_ids: list[str]
    commands_file: Optional[str] = None
    pre_runs_external_id_prefix: Optional[str] = None


class BidTimeFrame(BaseModel):
    """
    Used to dynamically specify what times to run the Dayahead bid process for in local time

    Args:
        shift_start_in_days: number of days from the current local time to shift the start time of the bid
        timezone: the local timezone to use when creating shop times
        bid_period_in_days: the number of days the bid is valid for. This number is used to produce the bid end time by
                            shifting the start time with this number
    """

    model_config = ConfigDict(frozen=True)

    shift_start_in_days: int
    timezone: Optional[str] = "Europe/Oslo"
    bid_period_in_days: Optional[int] = 14
    _datetime_string_format: str = "YYYY-MM-DD HH:mm:ss"
    _date_string_format: str = "YYYY-MM-DD"
    _start_time_arrow: arrow.Arrow

    @field_validator("bid_period_in_days")
    @classmethod
    def positive_bid_period_in_days(cls, value):
        if value < 0:
            raise ValueError("Bid period must be a positive number")

    @property
    def start_time_arrow(self):
        return (
            arrow.now(self.timezone).shift(days=self.shift_start_in_days).floor("day")
        )  # should floor be hour instead?

    @property
    def start_time_string(self) -> str:
        return self.start_time_arrow.format(self._datetime_string_format)

    @property
    def end_time_string(self) -> str:
        return (
            self.start_time_arrow.shift(days=self.bid_period_in_days).floor("week").format(self._datetime_string_format)
        )

    @property
    def bid_date(self) -> str:
        return self.start_time_arrow.shift(days=1).format(self._date_string_format)


class DayaheadTrigger(BaseModel):
    """
    Contains the blueprint of a Dayahead workflow definition. Used to trigger one or more cases within a price area
    along with SHOP prerun files (e.g. no preprocessing to the SHOP file before being run in CogSHOP)
    This object is used together with the powerops client (DayAheadTriggerAPI) to trigger a workflow from an instance
    of this class.

    Args:
        price_scenarios: list of price scenarios used in Dayahead run (do not have to exist in CDF, only appended as
                         metadata to the triggering events for reference)
        main_scenario: Main price scenario
        shop_version: SHOP Version to use in the run. The SHOP version must be installed to Cogshop or exist as a
                      binary file in CDF. For Mesh files specify a SHOP version > 15.4
        price_area: str
        method: Literal["multi_scenario", "price_independent"]
        bid_configuration_name: str
        cases: list of Case objects to run
        bid_time_frame: Optional[BidTimeFrame] = BidTimeFrame(shift_start_in_days=0)
        plant_names_override: Optional[dict] = None
        dayahead_configuration_external_id: Optional[str] = "market_configuration_nordpool_dayahead"
    """

    price_scenarios: list[str]
    main_scenario: str = ""
    shop_version: Optional[str] = _SHOP_VERSION_FALLBACK
    price_area: str
    method: Literal["multi_scenario", "price_independent"]
    bid_configuration_name: str
    cases: list[Case]
    bid_time_frame: Optional[BidTimeFrame] = BidTimeFrame(shift_start_in_days=0)
    plant_names_override: Optional[dict] = None
    dayahead_configuration_external_id: Optional[str] = "market_configuration_nordpool_dayahead"


class DayaheadWorkflowRun(BaseModel):
    """
    Return object when triggering a DayaheadTrigger workflow via the API.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    workflow_trigger_event: Event
    total_bid_event: Event
    partial_bid_events: list[Event]
    shop_run_events: list[Event]
