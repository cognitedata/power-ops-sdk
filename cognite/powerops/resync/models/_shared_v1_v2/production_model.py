from __future__ import annotations

from typing import Optional

import pandas as pd
from cognite.client.data_classes import Sequence

from cognite.powerops.resync.models.base import CDFSequence

p_min_fallback = 0.0
p_max_fallback = 100_000_000_000_000_000_00.0
head_loss_factor_fallback = 0.0


def _create_generator_efficiency_curve(generator_attributes, generator_name, generator_external_id) -> CDFSequence:
    x_col_name = "generator_power"
    y_col_name = "generator_efficiency"
    sequence = Sequence(
        external_id=f"{generator_external_id}_generator_efficiency_curve",
        name=f"{generator_name} generator efficiency curve",
        columns=[{"valueType": "DOUBLE", "externalId": x_col_name}, {"valueType": "DOUBLE", "externalId": y_col_name}],
    )
    data = generator_attributes["gen_eff_curve"]
    efficiency_curve = CDFSequence(
        sequence=sequence, content=pd.DataFrame({x_col_name: data["x"], y_col_name: data["y"]}, dtype=float)
    )
    return efficiency_curve


def _create_turbine_efficiency_curve(generator_attributes, generator_name, generator_external_id) -> CDFSequence:
    data = generator_attributes["turb_eff_curves"]
    ref_col_name = "head"
    x_col_name = "flow"
    y_col_name = "turbine_efficiency"
    sequence = Sequence(
        external_id=f"{generator_external_id}_turbine_efficiency_curve",
        name=f"{generator_name} turbine efficiency curve",
        columns=[
            {"valueType": "DOUBLE", "externalId": ref_col_name},
            {"valueType": "DOUBLE", "externalId": x_col_name},
            {"valueType": "DOUBLE", "externalId": y_col_name},
        ],
    )
    df = pd.DataFrame(
        {
            ref_col_name: [entry["ref"] for entry in data for _ in range(len(entry["x"]))],
            x_col_name: [item for entry in data for item in entry["x"]],
            y_col_name: [item for entry in data for item in entry["y"]],
        },
        dtype=float,
    )
    turbine_efficiency_curve = CDFSequence(sequence=sequence, content=df)
    return turbine_efficiency_curve


def _get_single_value(value_or_time_series: float | dict) -> float:
    """Get the single value from a time series, or a value
    returns the value if value_or_time_series is a value, otherwise the first value in the time series

    Parameters
    ----------
    value_or_time_series : float | dict
        Either a simple numeric value, or a dictionary (time series, {datetime string: value})
    """
    if isinstance(value_or_time_series, dict):
        return next(iter(value_or_time_series.values()))
    return value_or_time_series


def _plant_to_inlet_reservoir_breadth_first_search(
    plant_name: str, all_connections: list[dict], reservoirs: set[str]
) -> Optional[str]:
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
    Optional[str]
        The name of the reservoir connected to the plant, or None if no reservoir was found.
    """
    queue = []
    for connection in all_connections:
        if (
            connection["to"] == plant_name and connection.get("to_type", "plant") == "plant"
        ):  # if to_type is specified, it must be "plant"
            queue.append(connection)
            break
    visited = []
    while queue:
        connection = queue.pop(0)
        if connection not in visited:
            # Check if the given connection is from a reservoir
            # If we have "from_type" we can check directly if the object is a reservoir
            try:
                if connection["from_type"] == "reservoir":
                    return connection["from"]
            # If we don't have "from_type" we have to check if the name of the object is in the
            # list of reservoirs
            except KeyError:
                if connection["from"] in reservoirs:
                    return connection["from"]

            visited.append(connection)
            for candidate_connection in all_connections:
                # if the candidate connection is extension from the current connection, traverse it
                if candidate_connection["to"] == connection["from"]:
                    queue.append(candidate_connection)
    return None
