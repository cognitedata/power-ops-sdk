from __future__ import annotations

from cognite.powerops.resync.config import PriceScenario, PriceScenarioID, PriceScenarioV2


def _map_price_scenarios_by_name(
    scenario_ids: list[PriceScenarioID], price_scenarios_by_id: dict[str, PriceScenario], market_name: str
) -> dict[str, PriceScenario]:
    scenario_by_name = {}
    for identifier in scenario_ids:
        ref_scenario = price_scenarios_by_id[identifier.id]
        name = identifier.rename or ref_scenario.name or identifier.id
        scenario_by_name[name] = PriceScenario(name=market_name, **ref_scenario.model_dump(exclude={"name"}))
    return scenario_by_name


def _map_price_scenario_by_name_v2(
    scenario_ids: list[PriceScenarioID], price_scenarios_by_id: dict[str, PriceScenarioV2], market_name: str
) -> dict[str, PriceScenarioV2]:
    scenario_by_name = {}
    for identifier in scenario_ids:
        ref_scenario = price_scenarios_by_id[identifier.id]
        name = identifier.rename or ref_scenario.name or identifier.id
        scenario_by_name[name] = PriceScenario(name=market_name, **ref_scenario.model_dump(exclude={"name"}))
    return scenario_by_name
