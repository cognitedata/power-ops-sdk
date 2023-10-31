from pathlib import Path
from typing import Literal, Optional
from uuid import uuid4

from pydantic import BaseModel, field_validator, model_validator


class ShopRun(BaseModel):
    """
    Represents a single shop run based on a pre-run file.

    Question: should plants and price scenarios be read from cdf file?
              or beprovided directly by the user although file existst in cdf?
    """

    pre_run_path: Optional[Path]
    pre_run_external_id: Optional[str]
    plants: list[str]
    price_scenario: str

    @property
    def file_external_id_prefix(self) -> str:
        return f"cog_shop_manual/{uuid4()!s}"

    @model_validator(mode="after")
    def one_prerun_file_reference(self) -> "ShopRun":
        if (self.pre_run_path and self.pre_run_external_id) or (
            self.pre_run_path is None and self.pre_run_external_id is None
        ):
            raise ValueError("Specify one of either path to prerun file or external_id of file in CDF!")
        return self


class Case(BaseModel):
    """
    Case definition based on a set of prerun files to run for certain plants that are used in the set of prerun files
    """

    case_name: Optional[str]  # e.g. Glomma
    plants: list[str]  # list of all plants that are in case
    pre_runs: list[ShopRun]


class DayaheadTrigger(BaseModel):
    price_scenarios: list[str] = []
    main_scenario: str = ""
    price_area: str
    method: Literal["multi_scenario", "price_independent"]
    cases: list[Case]

    @field_validator("price_scenarios")
    @classmethod
    def price_scenario_available_for_method(cls, value):
        ...

    @property
    def num_price_areas(self) -> int:
        return len(self.price_scenarios)
