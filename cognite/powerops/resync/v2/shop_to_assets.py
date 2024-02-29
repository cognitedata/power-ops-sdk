# Mypy does not understand the pydantic classes that allows both alias and name to be used in population
# https://github.com/pydantic/pydantic/issues/3923
# mypy: disable-error-code="call-arg"
from __future__ import annotations

from math import floor, log10
from pathlib import Path
from typing import Any, Optional

from cognite.powerops.client._generated.v1.data_classes import (
    GeneratorEfficiencyCurveWrite,
    GeneratorWrite,
    PlantWrite,
    PriceAreaWrite,
    ReservoirWrite,
    TurbineEfficiencyCurveWrite,
    WatercourseWrite,
)
from cognite.powerops.utils.serialization import load_yaml

__all__ = ["PowerAssetImporter"]


class PowerAssetImporter:
    p_min_fallback = 0.0
    p_max_fallback = 10_000_000_000_000_000_000.0
    head_loss_factor_fallback = 0.0
    connection_losses_fallback = 0.0
    inlet_reservoir_fallback = ""
    default_shop_penalty_limit = 42000
    default_timezone = "Europe/Oslo"

    def __init__(
        self,
        shop_model_by_directory: dict[str, Any],
        generator_times_series_mappings: Optional[list[dict[str, Any]]] = None,
        plant_time_series_mappings: Optional[list[dict[str, Any]]] = None,
        watercourses: Optional[list[dict[str, Any]]] = None,
    ):
        self.shop_model_by_directory = shop_model_by_directory
        self.times_series_by_generator_name = {
            entry["generator_name"]: {k: str(v) for k, v in entry.items()}
            for entry in generator_times_series_mappings or []
        }
        self.times_series_by_plant_name = {
            entry["plant_name"]: {k: str(v) for k, v in entry.items()} for entry in plant_time_series_mappings or []
        }
        self.watercourse_by_directory = {entry["directory"]: entry for entry in watercourses or []}

    @classmethod
    def from_directory(cls, directory: Path, file_name: str = "model_raw") -> PowerAssetImporter:
        shop_model_files = list(directory.glob(f"**/{file_name}.yaml"))
        shop_model_by_watercourse = {
            file.parent.name: load_yaml(file, expected_return_type="dict") for file in shop_model_files
        }

        generator_mapping_file = directory / "generator_time_series_mappings.yaml"
        generator_mappings = (
            load_yaml(generator_mapping_file, expected_return_type="list") if generator_mapping_file.exists() else None
        )

        plant_mapping_file = directory / "plant_time_series_mappings.yaml"
        plant_mappings = (
            load_yaml(plant_mapping_file, expected_return_type="list") if plant_mapping_file.exists() else None
        )

        watercourses_file = directory / "watercourses.yaml"
        watercourses = load_yaml(watercourses_file, expected_return_type="list") if watercourses_file.exists() else None

        return cls(shop_model_by_watercourse, generator_mappings, plant_mappings, watercourses)

    def to_power_assets(self) -> list[GeneratorWrite | ReservoirWrite | PlantWrite | WatercourseWrite | PriceAreaWrite]:
        assets_by_xid: dict[str, GeneratorWrite | ReservoirWrite | PlantWrite | WatercourseWrite | PriceAreaWrite] = {}
        for watercourse_dir, shop_model in self.shop_model_by_directory.items():
            watercourse_assets = self._shop_model_to_watercourse_assets(shop_model, watercourse_dir, assets_by_xid)
            assets_by_xid.update(watercourse_assets)

        return list(assets_by_xid.values())

    def _shop_model_to_watercourse_assets(
        self,
        shop_model: dict[str, Any],
        watercourse_dir: str,
        existing: dict[str, GeneratorWrite | ReservoirWrite | PlantWrite | WatercourseWrite | PriceAreaWrite],
    ) -> dict[str, GeneratorWrite | ReservoirWrite | PlantWrite | WatercourseWrite | PriceAreaWrite]:
        assets_by_xid: dict[str, GeneratorWrite | ReservoirWrite | PlantWrite | WatercourseWrite | PriceAreaWrite] = {}
        try:
            watercourse_config = self.watercourse_by_directory[watercourse_dir]
        except KeyError as e:
            raise ValueError(
                f"Watercourse directory {watercourse_dir} does not exist in the watercourses.yaml configuration"
            ) from e
        watercourse_external_id = f"watercourse_{watercourse_config['name']}"
        plant_display_name_and_order = watercourse_config.get("plant_display_names_and_order", {})
        reservoir_display_name_and_order = watercourse_config.get("reservoir_display_names_and_order", {})

        generator_by_name = {}
        for name, data in shop_model["model"]["generator"].items():
            generator = self._to_generator(name, data)
            if generator.external_id in existing:
                raise ValueError(f"Generator with external id {generator.external_id} already exists")
            generator_by_name[generator.name] = generator
            assets_by_xid[generator.external_id] = generator

        reservoir_by_name = {}
        for name in shop_model["model"]["reservoir"].keys():
            reservoir = self._to_reservoir(name, reservoir_display_name_and_order)
            if reservoir.external_id in existing:
                raise ValueError(f"Reservoir with external id {reservoir.external_id} already exists")
            reservoir_by_name[reservoir.name] = reservoir
            assets_by_xid[reservoir.external_id] = reservoir

        plants = []
        for name, data in shop_model["model"]["plant"].items():
            plant = self._to_plant(
                name,
                data,
                plant_display_name_and_order,
                watercourse_external_id,
                reservoir_by_name,
                generator_by_name,
                shop_model,
            )
            if plant.external_id in existing:
                raise ValueError(f"Plant with external id {plant.external_id} already exists")
            plants.append(plant)
            assets_by_xid[plant.external_id] = plant

        data = watercourse_config
        watercourse = WatercourseWrite(
            external_id=watercourse_external_id,
            name=data["name"],
            production_obligation=data.get("production_obligation_ts_ext_ids", []),
            penalty_limit=data.get("shop_penalty_limit", self.default_shop_penalty_limit),
            plants=plants,
        )
        if watercourse.external_id in existing:
            raise ValueError(f"Watercourse with external id {watercourse.external_id} already exists")
        assets_by_xid[watercourse.external_id] = watercourse

        for price_area in watercourse_config.get("market_to_price_area", {}).values():
            if price_area in existing:
                raise ValueError(f"Price area with external id {price_area} already exists")
            price_area = PriceAreaWrite(
                external_id=f"price_area_{price_area}", name=price_area, timezone=self.default_timezone
            )
            assets_by_xid[price_area.external_id] = price_area

        return assets_by_xid

    def _to_generator(self, name: str, data: dict) -> GeneratorWrite:
        curve = data["gen_eff_curve"]
        efficiency_curve = GeneratorEfficiencyCurveWrite(
            external_id=f"{name}_efficiency_curve", ref=curve["ref"], power=curve["x"], efficiency=curve["y"]
        )
        turbine_curves = []
        for curve in data["turb_eff_curves"]:
            turbine_curve = TurbineEfficiencyCurveWrite(
                external_id=f"{name}_turbine_eff_curve_ref_{curve['ref']}",
                head=curve["ref"],
                flow=curve["x"],
                efficiency=curve["y"],
            )
            turbine_curves.append(turbine_curve)

        # Start cost is assumed to be a dictionary with (timestamp, value) key-value pairs
        # We assume it is constant and thus use only the first value.
        startcost = next(iter(data["startcost"].values()))

        generator_timeseries = self.times_series_by_generator_name.get(name, {})

        return GeneratorWrite(
            external_id=f"generator_{name}",
            name=name,
            p_min=data["p_min"],
            penstock=data["penstock"],
            start_cost=startcost,
            start_stop_cost=generator_timeseries.get("start_stop_cost"),
            is_available_time_series=generator_timeseries.get("is_available"),
            efficiency_curve=efficiency_curve,
            turbine_curves=turbine_curves,
        )

    def _to_plant(
        self,
        name: str,
        data: dict,
        plant_display_name_and_order: dict,
        watercourse_xid: str,
        reservoir_by_name: dict[str, ReservoirWrite],
        generator_by_name: dict[str, GeneratorWrite],
        shop_model: dict,
    ) -> PlantWrite:
        plant_timeseries = self.times_series_by_plant_name.get(name, {})
        display_name, order = plant_display_name_and_order.get(name, (name, 999))

        all_connections = shop_model["connections"]
        all_junctions = shop_model["model"].get("junction", {})
        all_tunnels = shop_model["model"].get("tunnel", {})
        inlet_reservoir_name, connection_losses = self._plant_to_inlet_reservoir_with_losses(
            name, all_connections, all_junctions, all_tunnels, set(reservoir_by_name.keys())
        )

        plant_generators: dict[str, GeneratorWrite] = {}
        for connection in all_connections:
            if (
                connection.get("from_type") == "plant"
                and connection["from"] == name
                and (gen := generator_by_name.get(connection["to"]))
            ):
                plant_generators[gen.name] = gen
            elif (
                connection.get("to_type") == "plant"
                and connection["to"] == name
                and (gen := generator_by_name.get(connection["from"]))
            ):
                plant_generators[gen.name] = gen

        return PlantWrite(
            external_id=f"plant_{name}",
            name=name,
            display_name=display_name,
            outlet_level=float(data.get("outlet_line", 0)),
            p_min=float(data.get("p_min", self.p_min_fallback)),
            p_max=float(data.get("p_max", self.p_max_fallback)),
            ordering=order,
            penstock_head_loss_factors={
                str(penstock_number): float(loss_factor)
                for penstock_number, loss_factor in enumerate(
                    data.get("penstock_loss", [self.head_loss_factor_fallback]), start=1
                )
            },
            head_loss_factor=float(data.get("main_loss", [self.head_loss_factor_fallback])[0]),
            connection_losses=connection_losses,
            water_value_time_series=plant_timeseries.get("water_value"),
            inlet_level_time_series=plant_timeseries.get("inlet_reservoir_level"),
            outlet_level_time_series=plant_timeseries.get("outlet_reservoir_level"),
            p_min_time_series=plant_timeseries.get("p_min"),
            p_max_time_series=plant_timeseries.get("p_max"),
            feeding_fee_time_series=plant_timeseries.get("feeding_fee"),
            head_direct_time_series=plant_timeseries.get("head_direct"),
            watercourse=watercourse_xid,
            inlet_reservoir=reservoir_by_name.get(inlet_reservoir_name),
            generators=list(plant_generators.values()),
        )

    @classmethod
    def _to_reservoir(cls, name: str, reservoir_display_name_and_order: dict) -> ReservoirWrite:
        display_name, order = reservoir_display_name_and_order.get(name, (name, 999))

        return ReservoirWrite(external_id=f"reservoir_{name}", name=name, display_name=display_name, ordering=order)

    # Todo - Refactor/Test coverage of this function:
    #  https://cognitedata.atlassian.net/browse/POWEROPS-2224?atlOrigin=eyJpIjoiNGFhYWQxMGU0NTE3NGEzNDlhZTBkN2Y5NDhkYTczYmYiLCJwIjoiaiJ9
    def _plant_to_inlet_reservoir_with_losses(
        self, plant_name: str, all_connections: list[dict], all_junctions: dict, all_tunnels: dict, reservoirs: set[str]
    ) -> tuple[str, float]:
        """Search for a reservoir connected to a plant, starting from the plant and searching breadth first.

        Parameters
        ----------
        plant_name : str
            The plant we want to find a connection from
        all_connections : list[dict]
            All connections in the model.
        reservoirs : dict
            All reservoirs in the model. Keys are reservoir names.

        Returns
        -------
        tuple[str, float]
            The name of the reservoir connected to the plant, or None if no reservoir was found.
        """

        def get_connection_path_from_last_visited(visited_paths: list, last_visited_id: int) -> list | None:
            """Return the correct sequence of visited connections based on the last visited connection among
            the list of connection paths visited

                Parameters
                ----------
                visited_paths : list
                     A list that holds the lists of each visited connection path. A connection path will be a list of
                     connection IDs, e.g. [1,2,3] means connection with ID 1 was first visited, then 2, then 3
                last_visited_id: int
                    The ID of the connection that was last visited. This will be the last item in one of the lists of
                    visited paths

                Returns
                -------
                list
                    The path or sequence of connections that has the last_visited_id as its
                    last visited connection among the visited_paths
            """
            for connection in visited_paths:
                if connection[-1] == last_visited_id:
                    connection_path_index = visited_paths.index(connection)
                    return visited_paths[connection_path_index]
            return None

        def calculate_losses_from_connection_path(
            all_junctions: dict, all_tunnels: dict, connection_by_id: dict, connection_path: list[int]
        ):
            """Loop through connections in the connection path, retrieve the losses for that connection among the
            all_junctions or all_tunnels based on the type of connection, and sum up the total losses from
            the connection path
            """
            sum_losses = 0
            order_to_loss_factor_key = {0: "loss_factor_1", 1: "loss_factor_2"}
            for connection_id in connection_path:
                connection = connection_by_id[connection_id]
                connection_name = connection["to"]
                if connection_name in all_junctions:
                    if connection.get("order") in order_to_loss_factor_key:
                        junction_losses = all_junctions[connection_name]
                        loss_order = connection["order"]
                        sum_losses += junction_losses[order_to_loss_factor_key[loss_order]]
                elif connection_name in all_tunnels:
                    tunnel_loss = all_tunnels[connection_name].get("loss_factor", 0)
                    sum_losses += tunnel_loss
            return sum_losses

        queue = []
        connection_by_id = dict(enumerate(all_connections))
        track_connection_paths = []
        last_connection_id = None

        for connection_id, connection in connection_by_id.items():
            if (
                connection["to"] == plant_name and connection.get("to_type", "plant") == "plant"
            ):  # if to_type is specified, it must be "plant"
                queue.append((connection_id, connection))
                track_connection_paths.append([connection_id])  # add the first connection to the path
                break
        visited = []
        while queue:
            connection_id, connection = queue.pop(0)
            if connection not in visited:
                # Check if the given connection is from a reservoir
                # If we have "from_type" we can check directly if the object is a reservoir
                try:
                    if connection["from_type"] == "reservoir":
                        inlet_reservoir = connection["from"]
                        last_connection_id = connection_id
                        break
                # If we don't have "from_type" we have to check if the name of the object is in the
                # list of reservoirs
                except KeyError:
                    if connection["from"] in reservoirs:
                        inlet_reservoir = connection["from"]
                        last_connection_id = connection_id
                        break

                visited.append(connection)
                for candidate_connection_id, candidate_connection in connection_by_id.items():
                    # if the candidate connection is extension from the current connection, traverse it
                    if candidate_connection["to"] == connection["from"]:
                        queue.append((candidate_connection_id, candidate_connection))
                        new_path_list = get_connection_path_from_last_visited(track_connection_paths, connection_id)
                        if new_path_list:
                            track_connection_paths.append([*new_path_list, candidate_connection_id])

        if last_connection_id is None:
            return self.inlet_reservoir_fallback, self.connection_losses_fallback  # TODO: raise an error here instead?

        connection_path = get_connection_path_from_last_visited(track_connection_paths, last_connection_id) or []

        connection_losses = calculate_losses_from_connection_path(
            all_junctions, all_tunnels, connection_by_id, connection_path
        )

        return (inlet_reservoir, self._round_sig(connection_losses, 4) if connection_losses else connection_losses)

    @staticmethod
    def _round_sig(x: float, sig: int = 2):
        return round(x, sig - int(floor(log10(abs(x)))) - 1)
