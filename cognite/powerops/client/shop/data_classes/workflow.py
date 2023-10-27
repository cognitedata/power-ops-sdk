from pathlib import Path
from typing import Literal

from pydantic import BaseModel, field_validator

class ShopCase(BaseModel):
    pre_run_file: Path | str
    plants: list[str]
    price_scenario: str

class Case(BaseModel):
    case_name: str # e.g. Glomma
    plants: list[str]
    pre_runs: list[ShopCase]

class Workflow(BaseModel):
    market: str = "Dayahead"
    price_scenarios: list[str] #(or extract these from the SHOPCases?) good for readability tho
    price_area: str
    method: Literal["multi_scenario", "price_independent"]
    cases: list[Case]

    @field_validator("price_scenarios") # validate that pricescenarios are available for a given method
    def price_scenario_available_for_method(self, value):
        ...
