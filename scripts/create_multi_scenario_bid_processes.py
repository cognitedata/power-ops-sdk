from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass
class RelativeDatapoint:
    offset_minute: float
    offset_value: float


@dataclass
class Scenario:
    name: str
    time_series_external_id: str
    tranformations: dict[str, list[RelativeDatapoint]]

    def to_dict_v1(self) -> dict:
        return {
            "name": self.name,
            "time_series_external_id": self.time_series_external_id,
            "transformations": [
                {
                    "kwargs": {rdp.offset_minute: int(rdp.offset_value) for rdp in rpds},
                    "transformation": "".join(["_" + c.lower() if c.isupper() else c for c in trans])
                    .lstrip("_")
                    .upper(),
                }
                for trans, rpds in self.tranformations.items()
            ],
        }

    def to_dict_v2(self) -> dict:

        return {
            "name": self.name,
            "time_series_external_id": self.time_series_external_id,
            "transformations": [
                {
                    trans: {"parameters": {"relative_datapoints": [rdp.__dict__ for rdp in rpds]}}
                    for trans, rpds in self.tranformations.items()
                }
            ],
        }


@dataclass
class BidProcess:
    bid_matrix_generator: str
    main_scenario: Scenario
    name: str
    no_shop: bool
    price_area_name: str
    price_scenarios: list[Scenario]

    def to_dict(self):
        return {
            "bid_matrix_generator": self.bid_matrix_generator,
            "main_scenario": self.main_scenario.name,
            "name": self.name,
            "no_shop": self.no_shop,
            "price_area_name": self.price_area_name,
            "price_scenarios": [{"id": scenario.name} for scenario in self.price_scenarios],
        }


def generate_price_scenarios(num_scenarios: int) -> list[Scenario]:
    price_area = "NO2"
    transformation = "AddFromOffset"
    range_start, range_end = -500, 2000

    step_size = (range_end - range_start) / (num_scenarios - 1)
    offsets = [int(range_start + i * step_size) for i in range(1, num_scenarios - 1)]
    offsets = [range_start] + offsets + [range_end]

    scenarios = []

    for offset in offsets:
        scenario_id = f"multi_scenario_{num_scenarios}_{price_area}_{offset}"

        relative_datapoints = [
            RelativeDatapoint(offset_minute=0.0, offset_value=float(offset)),
            RelativeDatapoint(offset_minute=1440.0, offset_value=0.0),
        ]

        scenario = Scenario(
            name=scenario_id,
            time_series_external_id="907677",
            tranformations={transformation: relative_datapoints},
        )

        scenarios.append(scenario)

    return scenarios


def generate_bid_process(scenarios: list[Scenario]) -> BidProcess:
    num_scenarios = len(scenarios)
    main_scenario = scenarios[num_scenarios // 2]

    price_area_name = "NO2"
    name = f"multi_scenario_{num_scenarios}_{price_area_name}"

    return BidProcess(
        bid_matrix_generator="multi_scenario",
        main_scenario=main_scenario,
        name=name,
        no_shop=False,
        price_area_name=price_area_name,
        price_scenarios=scenarios,
    )


def write_between_comments(file_path: Path, data: str):

    script_path = "/scripts/create_multi_scenario_bid_processes.py"

    start_comment = f"# START: Generated by {script_path}"
    end_comment = f"# END: Generated by {script_path}"

    with open(file_path, "r") as file:
        lines = file.readlines()

    start_index, end_index = None, None
    for i, line in enumerate(lines):
        if start_comment in line:
            start_index = i
        elif end_comment in line:
            end_index = i

    if start_index is None or end_index is None:
        raise ValueError("Start or end comment not found in the file.")

    lines = lines[: start_index + 1] + [f"{data}"] + lines[end_index:]

    with open(file_path, "w") as file:
        file.writelines(lines)


def create_multi_scenario_demo_config(num_scenarios: int) -> tuple[BidProcess, list[Scenario]]:

    price_scenarios: list[Scenario] = generate_price_scenarios(num_scenarios)
    bid_process: BidProcess = generate_bid_process(price_scenarios)

    return bid_process, price_scenarios


def create_multi_scenario_demo_configs(
    num_scenarios_list: list[int], scenarios_file: Path, scenarios_file_v2: Path, bid_process_file: Path
):

    bid_processes: list[BidProcess] = []
    scenarios: list[Scenario] = []

    for num_scenarios in num_scenarios_list:
        bid_process, scenario = create_multi_scenario_demo_config(num_scenarios)
        bid_processes.append(bid_process)
        scenarios.extend(scenario)

    price_scenarios_dict = {scenario.name: scenario.to_dict_v1() for scenario in scenarios}
    price_scenarios_dict_v2 = {scenario.name: scenario.to_dict_v2() for scenario in scenarios}
    bid_process_list = [bid_process.to_dict() for bid_process in bid_processes]

    write_between_comments(scenarios_file, yaml.dump(price_scenarios_dict, sort_keys=False))
    write_between_comments(scenarios_file_v2, yaml.dump(price_scenarios_dict_v2, sort_keys=False))
    write_between_comments(bid_process_file, yaml.dump(bid_process_list, sort_keys=False))


if __name__ == "__main__":

    # This script will generate the configuration files for the multi-scenario demo
    # This is primarily used for testing SHOP As A Service

    scenarios_file = Path.cwd().parent / "tests" / "data" / "demo" / "market" / "price_scenario_by_id.yaml"
    scenarios_v2_file = Path.cwd().parent / "tests" / "data" / "demo" / "market" / "price_scenario_by_id_v2.yaml"
    bid_process_file = Path.cwd().parent / "tests" / "data" / "demo" / "market" / "dayahead" / "bidprocess.yaml"

    num_scenarios_list = [50, 100, 200, 400, 800]

    create_multi_scenario_demo_configs(num_scenarios_list, scenarios_file, scenarios_v2_file, bid_process_file)
