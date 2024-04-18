import hashlib
import json

from pathlib import Path

import yaml

from cognite.powerops.resync.config import ReSyncConfig, Transformation
from cognite.powerops.resync.models._shared_v1_v2.cogshop_model import transformations_v2_transformer


def hash_dictionary(d: dict) -> str:
    s_hashable = json.dumps(d).encode("utf-8")
    m = hashlib.sha256(s_hashable).hexdigest()
    return m


def generate_new_time_series_mappings(old_time_series_mappings: list[dict], write_path: Path):
    new_time_series_mapping: list[dict] = []

    for mapping in old_time_series_mappings:
        new_mapping = {"rows": []}
        for mapping_entry in mapping["rows"]:
            new_transformations = []
            object_name = mapping_entry.get("object_name")
            object_type = mapping_entry.get("object_type")
            new_mapping_entry = mapping_entry.copy()
            if transformations := mapping_entry.get("transformations"):
                for transformation in transformations:
                    old_transformation = Transformation(**transformation)
                    new_transformation = transformations_v2_transformer(
                        old_transformation, object_name=object_name, object_type=object_type
                    )
                    new_transformations.append(
                        {new_transformation.name: {"parameters": new_transformation.model_dump()}}
                    )
                new_mapping_entry["transformations"] = new_transformations
            new_mapping["rows"].append(new_mapping_entry)

        new_time_series_mapping.append(new_mapping)

    # write to yaml file
    write_path.write_text(yaml.safe_dump(new_time_series_mapping))

    print("--- New time series mapping file is up to date with new transformations ---")


def generate_new_price_scenarios_mappings(old_price_scenarios_mappings: dict, write_path: Path):
    new_price_scenarios = {}

    for price_id, price_scenario in old_price_scenarios_mappings.items():
        new_scenario = price_scenario.model_dump()
        if price_scenario.transformations:
            new_transformations = []
            for t in price_scenario.transformations:
                new_transformation = transformations_v2_transformer(t)
                new_transformations.append({new_transformation.name: {"parameters": new_transformation.model_dump()}})
            new_scenario["transformations"] = new_transformations
            new_price_scenarios[price_id] = new_scenario
            continue
        new_price_scenarios[price_id] = new_scenario

    # write to yaml file
    write_path.write_text(yaml.safe_dump(new_price_scenarios))

    print("--- New price scenarios file is up to date with new transformations ---")


def _create_transformations_file(
    old_time_series_mappings: list[dict], old_price_scenarios_mappings: dict, write_path: Path
):
    transofrmations_v2 = {"transformations": []}
    transformations_cache = set()

    for mapping in old_time_series_mappings:
        for mapping_entry in mapping["rows"]:
            if transformations := mapping_entry.get("transformations"):
                new_transformations = []
                object_name = mapping_entry.get("object_name")
                object_type = mapping_entry.get("object_type")
                for transformation in transformations:
                    old_transformation = Transformation(**transformation)
                    new_transformation = transformations_v2_transformer(
                        old_transformation, object_name=object_name, object_type=object_type
                    )
                    new_transformation_dict = {new_transformation.name: {"parameters": new_transformation.model_dump()}}
                    d_hash = hash_dictionary(new_transformation_dict)
                    if d_hash not in transformations_cache:
                        transformations_cache.add(d_hash)
                        new_transformations.append(new_transformation_dict)
                transofrmations_v2["transformations"].extend(new_transformations)

    for price_scenario in old_price_scenarios_mappings.values():
        if price_scenario.transformations:
            new_transformations = []
            for t in price_scenario.transformations:
                new_transformation = transformations_v2_transformer(t)
                new_transformation_dict = {new_transformation.name: {"parameters": new_transformation.model_dump()}}
                d_hash = hash_dictionary(new_transformation_dict)
                if d_hash not in transformations_cache:
                    transformations_cache.add(d_hash)
                    new_transformations.append(new_transformation_dict)
            transofrmations_v2["transformations"].extend(new_transformations)

    print(f"{len(transformations_cache)} number of new unique transformations extracted from config files")
    # write to yaml file
    write_path.write_text(yaml.safe_dump(transofrmations_v2))


def create_new_config_files_with_transformations_v2(configs_path: Path, cdf_project: str):
    config = ReSyncConfig.from_yamls(configs_path, cdf_project)

    old_time_series_mappings = [mapping.dumps() for mapping in config.cogshop.time_series_mappings]
    old_price_scenario_mappings = config.market.price_scenario_by_id

    generate_new_price_scenarios_mappings(
        old_price_scenario_mappings, configs_path / "market" / "price_scenario_by_id_v2.yaml"
    )
    generate_new_time_series_mappings(
        old_time_series_mappings, configs_path / "cogshop" / "time_series_mappings_v2.yaml"
    )


def create_transformationsV2_file(configs_path: Path, write_path: Path, cdf_project: str):
    config = ReSyncConfig.from_yamls(configs_path, cdf_project)

    old_time_series_mappings = [mapping.dumps() for mapping in config.cogshop.time_series_mappings]
    old_price_scenario_mappings = config.market.price_scenario_by_id

    _create_transformations_file(
        old_time_series_mappings, old_price_scenario_mappings, write_path / "transformations_v2.yaml"
    )

