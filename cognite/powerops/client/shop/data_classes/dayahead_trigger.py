from typing import Literal, Optional

from pydantic import BaseModel


class DayaheadTriggerEvent:
    event_type: str = "POWEROPS_BID_PROCESS_FROM_PRERUNS"
    external_id_prefix: str = "POWEROPS_BID_PROCESS_"
    market: str = "bid:market"
    main_scenario: str = "bid:main_scenario"
    price_scenarios: str = "bid:price_scenarios"
    method: str = "bid:method"
    price_area: str = "bid:price_area"
    relationship_label_to_shop_run_events: str = "relationship_to.shop_run_event"


class ShopRun(BaseModel):
    """
    Represents a single shop run based on a pre-run file.

    Question: should plants and price scenarios be read from cdf file?
              or beprovided directly by the user although file existst in cdf?
    """

    pre_run_external_id: str
    plants: Optional[list[str]] = None  # all the plants included in this ShopRun
    price_scenario: Optional[str] = None


class Case(BaseModel):
    """
    Case definition based on a set of prerun files to run for certain plants that are used in the set of prerun files
    """

    case_name: str  # e.g. Glomma
    pre_runs: list[ShopRun]
    plants: list[str] = []
    commands_file: Optional[str] = None  # most prerun files do not have commands. Provide this as extra files
    pre_runs_external_id_prefix: Optional[str] = None


class DayaheadTrigger(BaseModel):
    price_scenarios: list[str]
    main_scenario: str = ""
    price_area: str
    method: Literal["multi_scenario", "price_independent", "Gajas superawesome bid config"]
    cases: list[Case]

    @property
    def num_price_areas(self) -> int:
        return len(self.price_scenarios)
