from collections import defaultdict

from typing import Dict, List

from cognite.powerops.data_classes.common import RetrievalType
from cognite.powerops.data_classes.time_series_mapping import TimeSeriesMapping, TimeSeriesMappingEntry
from cognite.powerops.data_classes.transformation import Transformation, TransformationType
from cognite.powerops.utils.common import print_warning, replace_nordic_letters
from cognite.powerops.utils.mapping.tco_mapping import load_tco, parse_sim_key, parse_tco_lines


def find_objects_with_time_delay(shop_model: dict) -> Dict[str, list]:
    """Find SHOP objects in model that has time delay"""
    objects = defaultdict(list)
    for object_type in ["plant", "gate"]:
        for shop_object, attributes in shop_model[object_type].items():
            if "time_delay" in attributes or "shape_discharge" in attributes:
                objects[object_type].append(shop_object)
    return objects


def group_objects_per_reservoir(objects: Dict[str, list], connections: List[dict]) -> Dict[str, list]:
    """Creates a mapping from reservoir name to SHOP object name based on the connections"""
    grouped = defaultdict(list)
    for object_names in objects.values():
        for object_name in object_names:
            for connection in connections:
                if connection["from"] == object_name and connection["to_type"] == "reservoir":
                    reservoir_name = connection["to"]
                    grouped[reservoir_name].append(object_name)
    return grouped


# ! assumes no nordic letters in shop_model !
# TODO: can probably loop more efficiently / directly without using functions above..
# TODO: tco should be loaded outside?? -> just pass tco_pairs: tuple
def inflow_with_water_in_transit_mappings(
    hist_flow_time_series: Dict[str, str],
    shop_model: dict,
    shop_connections: List[dict],
    tco_paths: List[str],
) -> TimeSeriesMapping:
    objects_with_time_delay = find_objects_with_time_delay(shop_model=shop_model)
    objects_with_time_delay_per_reservoir = group_objects_per_reservoir(
        objects=objects_with_time_delay, connections=shop_connections
    )

    sim_key_and_dbi_key = []
    for tco_path in tco_paths:  # TODO: handle multiple in tco.py
        tco_lines = load_tco(tco_path)
        pairs = parse_tco_lines(tco_lines)
        sim_key_and_dbi_key.extend(pairs)

    mappings = TimeSeriesMapping()
    for reservoir_name, objects in objects_with_time_delay_per_reservoir.items():
        for sim_key, dbi_key in sim_key_and_dbi_key:
            _, sim_object_name, sim_attribute_name = parse_sim_key(sim_key)

            if replace_nordic_letters(sim_object_name) == reservoir_name and sim_attribute_name == "inflow":
                transformations = []

                for object_name in objects:
                    if object_name not in hist_flow_time_series:
                        continue

                    kwargs = {"external_id": hist_flow_time_series[object_name]}
                    if object_name in objects_with_time_delay["gate"]:
                        kwargs["gate_name"] = object_name

                    elif object_name in objects_with_time_delay["plant"]:
                        kwargs["plant_name"] = object_name

                    transformations.append(
                        Transformation(transformation=TransformationType.ADD_WATER_IN_TRANSIT, kwargs=kwargs)
                    )

                if transformations:
                    mappings.append(
                        TimeSeriesMappingEntry(
                            object_type="reservoir",
                            object_name=reservoir_name,
                            attribute_name="inflow",
                            time_series_external_id=dbi_key,
                            transformations=transformations,
                            retrieve=RetrievalType.RANGE,
                        )
                    )

                else:
                    print_warning(
                        f"No historical flow time series found for reservoir {reservoir_name}'s upstreams "
                        f"gates/plants with time delay: {objects}"
                    )

                break

    return mappings
