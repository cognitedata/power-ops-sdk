# Mypy does not understand the pydantic classes that allows both alias and name to be used in population
# https://github.com/pydantic/pydantic/issues/3923
# mypy: disable-error-code="call-arg,attr-defined"
# TODO: remove attr-defined when updated to new data model


# from cognite.powerops.client._generated.v1.data_classes import (
#     GeneratorEfficiencyCurveWrite,
#     GeneratorWrite,
#     PlantWrite,
#     PriceAreaWrite,
#     ReservoirWrite,
#     TurbineEfficiencyCurveWrite,
#     WatercourseWrite,


# class PowerAssetImporter:

#     def __init__(
#         self,
#         shop_model_by_directory: dict[str, Any],
#     ):
#         self.times_series_by_generator_name = {
#             for entry in generator_times_series_mappings or []
#         self.times_series_by_plant_name = {
#             entry["plant_name"]: {k: str(v) for k, v in entry.items()} for entry in plant_time_series_mappings or []

#     @classmethod
#     def from_directory(cls, directory: Path, file_name: str = "model_raw") -> PowerAssetImporter:
#             file.parent.name: load_yaml(file, expected_return_type="dict") for file in shop_model_files


#     def to_power_assets(
#       self) -> list[GeneratorWrite | ReservoirWrite | PlantWrite | WatercourseWrite | PriceAreaWrite]:
#         for watercourse_dir, shop_model in self.shop_model_by_directory.items():


#     def _shop_model_to_watercourse_assets(
#         self,
#         shop_model: dict[str, Any],
#         watercourse_dir: str,
#         existing: dict[str, GeneratorWrite | ReservoirWrite | PlantWrite | WatercourseWrite | PriceAreaWrite],
#     ) -> dict[str, GeneratorWrite | ReservoirWrite | PlantWrite | WatercourseWrite | PriceAreaWrite]:
#             raise ValueError(
#             ) from e

#         for name, data in shop_model["model"]["generator"].items():
#             if generator.external_id in existing:

#         for name in shop_model["model"]["reservoir"].keys():
#             if reservoir.external_id in existing:

#         for name, data in shop_model["model"]["plant"].items():
#                 name,
#                 data,
#                 plant_display_name_and_order,
#                 watercourse_external_id,
#                 reservoir_by_name,
#                 generator_by_name,
#                 shop_model,
#             if plant.external_id in existing:

#         if watercourse.external_id in existing:

#         for price_area in watercourse_config.get("market_to_price_area", {}).values():
#             if price_area in existing:


#     def _to_generator(self, name: str, data: dict) -> GeneratorWrite:
#         for curve in data["turb_eff_curves"]:

#         # Start cost is assumed to be a dictionary with (timestamp, value) key-value pairs
#         # We assume it is constant and thus use only the first value.


#         return GeneratorWrite(

#     def _to_plant(
#         self,
#         name: str,
#         data: dict,
#         plant_display_name_and_order: dict,
#         watercourse_xid: str,
#         reservoir_by_name: dict[str, ReservoirWrite],
#         generator_by_name: dict[str, GeneratorWrite],
#         shop_model: dict,
#     ) -> PlantWrite:


#         for connection in all_connections:
#             if (
#                 and connection["from"] == name
#                 and (gen := generator_by_name.get(connection["to"]))
#             ):
#             elif (
#                 and connection["to"] == name
#                 and (gen := generator_by_name.get(connection["from"]))
#             ):

#         return PlantWrite(
#                 for penstock_number, loss_factor in enumerate(
#             },

#     @classmethod
#     def _to_reservoir(cls, name: str, reservoir_display_name_and_order: dict) -> ReservoirWrite:


#     # Todo - Refactor/Test coverage of this function:
#     #  https://cognitedata.atlassian.net/browse/POWEROPS-2224?atlOrigin=eyJpIjoiNGFhYWQxMGU0NTE3NGEzNDlhZTBkN2Y5NDhkYTczYmYiLCJwIjoiaiJ9
#     def _plant_to_inlet_reservoir_with_losses(
#         self,
#         plant_name: str, all_connections: list[dict], all_junctions: dict, all_tunnels: dict, reservoirs: set[str]
#     ) -> tuple[str, float]:
#         """Search for a reservoir connected to a plant, starting from the plant and searching breadth first.

#         Parameters
#         ----------
#             The plant we want to find a connection from
#             All connections in the model.
#             All reservoirs in the model. Keys are reservoir names.

#         Returns
#         -------
#             The name of the reservoir connected to the plant, or None if no reservoir was found.
#         """

#         def get_connection_path_from_last_visited(visited_paths: list, last_visited_id: int) -> list | None:
#             """Return the correct sequence of visited connections based on the last visited connection among
#             the list of connection paths visited

#                 Parameters
#                 ----------
#                      A list that holds the lists of each visited connection path. A connection path will be a list of
#                      connection IDs, e.g. [1,2,3] means connection with ID 1 was first visited, then 2, then 3
#                     The ID of the connection that was last visited. This will be the last item in one of the lists of
#                     visited paths

#                 Returns
#                 -------
#                 list
#                     The path or sequence of connections that has the last_visited_id as its
#                     last visited connection among the visited_paths
#             """
#             for connection in visited_paths:
#                 if connection[-1] == last_visited_id:

#         def calculate_losses_from_connection_path(
#             all_junctions: dict, all_tunnels: dict, connection_by_id: dict, connection_path: list[int]
#         ):
#             """Loop through connections in the connection path, retrieve the losses for that connection among the
#             all_junctions or all_tunnels based on the type of connection, and sum up the total losses from
#             the connection path
#             """
#             for connection_id in connection_path:
#                 if connection_name in all_junctions:
#                     if connection.get("order") in order_to_loss_factor_key:


#         for connection_id, connection in connection_by_id.items():
#             if (
#             ):  # if to_type is specified, it must be "plant"
#         while queue:
#             if connection not in visited:
#                 # Check if the given connection is from a reservoir
#                 # If we have "from_type" we can check directly if the object is a reservoir
#                     if connection["from_type"] == "reservoir":
#                 # If we don't have "from_type" we have to check if the name of the object is in the
#                 # list of reservoirs
#                     if connection["from"] in reservoirs:

#                 for candidate_connection_id, candidate_connection in connection_by_id.items():
#                     # if the candidate connection is extension from the current connection, traverse it
#                     if candidate_connection["to"] == connection["from"]:
#                         if new_path_list:

#         if last_connection_id is None:


#             all_junctions, all_tunnels, connection_by_id, connection_path


#     @staticmethod
#     def _round_sig(x: float, sig: int = 2):
