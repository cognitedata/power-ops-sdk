from typing import Optional

from pydantic import BaseModel
from typing_extensions import Self

_SHOP_VERSION_FALLBACK = "15.3.3.2"


class PrerunFileMetadata:
    plants: str = "shop:plants"
    price_scenario: str = "shop:price_scenario"
    _plants_delimiter: str = ","


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
    plants: list[str] = []
    commands_file: Optional[str] = None  # most prerun files do not have commands. Provide this as extra files
    pre_runs_external_id_prefix: Optional[str] = None


class DayaheadTrigger(BaseModel):
    price_scenarios: list[str]
    main_scenario: str = ""
    shop_version: Optional[str] = _SHOP_VERSION_FALLBACK
    price_area: str
    method: str
    cases: list[Case]

    @property
    def num_price_areas(self) -> int:
        return len(self.price_scenarios)
