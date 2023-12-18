from pathlib import Path

import yaml

from cognite.powerops.resync.config import ReSyncConfig, Transformation
from cognite.powerops.resync.models._shared_v1_v2.cogshop_model import transformations_v2_transformer

REPO_ROOT = Path(__file__).resolve().parent.parent


def generate_new_time_series_mappings(old_time_series_mappings: dict, write_path: Path):
    new_time_series_mapping = [{"rows": []}]

    for mapping in old_time_series_mappings["rows"]:
        if transformations := mapping.get("transformations"):
            new_transformations = []
            object_name = mapping.get("object_name")
            object_type = mapping.get("object_type")
            for transformation in transformations:
                old_transformation = Transformation(**transformation)
                new_transformation = transformations_v2_transformer(
                    old_transformation, object_name=object_name, object_type=object_type
                )
                new_transformations.append({new_transformation.name: {"input": new_transformation.model_dump()}})
            new_mapping = mapping.copy()
            new_mapping["transformations"] = new_transformations
            new_time_series_mapping[0]["rows"].append(new_mapping)
            continue
        new_time_series_mapping[0]["rows"].append(mapping)

    # write to yaml file
    with open(write_path, "w") as f:
        yaml.dump(new_time_series_mapping, f)


def generate_new_price_scenarios_mappings(old_price_scenarios_mappings: dict, write_path: Path):
    new_price_scenarios = {}

    for price_id, price_scenario in old_price_scenarios_mappings.items():
        new_scenario = price_scenario.model_dump()
        if price_scenario.transformations:
            new_transformations = []
            for t in price_scenario.transformations:
                new_transformation = transformations_v2_transformer(t)
                new_transformations.append({new_transformation.name: {"input": new_transformation.model_dump()}})
            new_scenario["transformations"] = new_transformations
            new_price_scenarios[price_id] = new_scenario
            continue
        new_price_scenarios[price_id] = new_scenario

    # write to yaml file
    with open(write_path, "w") as f:
        yaml.dump(new_price_scenarios, f)


def create_new_transformations_file(
    old_time_series_mappings: dict, old_price_scenarios_mappings: dict, write_path: Path
):
    transofrmations_v2 = {"transformations": []}

    for mapping in old_time_series_mappings["rows"]:
        if transformations := mapping.get("transformations"):
            new_transformations = []
            object_name = mapping.get("object_name")
            object_type = mapping.get("object_type")
            for transformation in transformations:
                old_transformation = Transformation(**transformation)
                new_transformation = transformations_v2_transformer(
                    old_transformation, object_name=object_name, object_type=object_type
                )
                new_transformations.append({new_transformation.name: {"input": new_transformation.model_dump()}})
            transofrmations_v2["transformations"].extend(new_transformations)

    for price_id, price_scenario in old_price_scenarios_mappings.items():
        new_scenario = price_scenario.model_dump()
        if price_scenario.transformations:
            new_transformations = []
            for t in price_scenario.transformations:
                new_transformation = transformations_v2_transformer(t)
                new_transformations.append({new_transformation.name: {"input": new_transformation.model_dump()}})
            transofrmations_v2["transformations"].extend(new_transformations)

    # write to yaml file
    with open(write_path, "w") as f:
        yaml.dump(transofrmations_v2, f)


if __name__ == "__main__":
    read_path = REPO_ROOT / "tests" / "data" / "demo"
    write_path = REPO_ROOT / "tests" / "data" / "generated"
    cdf_project = "powerops-staging"

    config = ReSyncConfig.from_yamls(read_path, cdf_project)

    old_time_series_mappings = config.cogshop.time_series_mappings[0].dumps()
    old_price_scenario_mappings = config.market.price_scenario_by_id

    generate_new_price_scenarios_mappings(old_price_scenario_mappings, write_path / "price_scenarios_by_id_v2.yaml")
    generate_new_time_series_mappings(old_time_series_mappings, write_path / "time_series_mappings_v2.yaml")
    create_new_transformations_file(
        old_time_series_mappings, old_price_scenario_mappings, write_path / "transformations_v2.yaml"
    )
