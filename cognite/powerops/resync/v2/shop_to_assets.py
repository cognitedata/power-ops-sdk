#!/usr/bin/env python
# Mypy does not understand the pydantic classes that allows both alias and name to be used in population
# type: ignore # noqa: [call-arg]
from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelWrite,
    GeneratorEfficiencyCurveWrite,
    GeneratorWrite,
    PlantWrite,
    TurbineEfficiencyCurveWrite,
)
from cognite.powerops.utils.serialization import load_yaml


class PowerAssetImporter:
    p_min_fallback = 0.0
    p_max_fallback = 100_000_000_000_000_000_00.0
    head_loss_factor_fallback = 0.0
    connection_losses_fallback = 0.0
    inlet_reservoir_fallback = ""

    def __init__(
        self,
        shop_models: list[dict],
        generator_times_series_mappings: Optional[list[dict]] = None,
        plant_time_series_mappings: Optional[list[dict]] = None,
    ):
        self.shop_models = shop_models
        self.times_series_by_generator_name = {
            entry["generator_name"]: {k: str(v) for k, v in entry.items()}
            for entry in generator_times_series_mappings or []
        }
        self.times_series_by_plant_name = {
            entry["plant_name"]: {k: str(v) for k, v in entry.items()} for entry in plant_time_series_mappings or []
        }

    @classmethod
    def from_directory(cls, directory: Path, file_name: str = "model_raw") -> PowerAssetImporter:
        shop_model_files = list(directory.glob(f"**/{file_name}.yaml"))
        shop_models = [load_yaml(file) for file in shop_model_files]

        generator_mapping_file = directory / "generator_time_series_mappings.yaml"
        generator_mappings = load_yaml(generator_mapping_file) if generator_mapping_file.exists() else None

        plant_mapping_file = directory / "plant_time_series_mappings.yaml"
        plant_mappings = load_yaml(plant_mapping_file) if plant_mapping_file.exists() else None

        return cls(shop_models, generator_mappings, plant_mappings)

    def to_power_assets(self) -> list[DomainModelWrite]:
        assets_by_xid: dict[str, DomainModelWrite] = {}
        for shop_model in self.shop_models:
            assets_by_xid.update(self._model_to_assets(shop_model, assets_by_xid))

        return list(assets_by_xid.values())

    def _model_to_assets(
        self, shop_model: dict[str, Any], existing: dict[str, DomainModelWrite]
    ) -> dict[str, DomainModelWrite]:
        assets_by_xid: dict[str, DomainModelWrite] = {}
        for name, data in shop_model["model"]["generator"].items():
            generator = self._to_generator(name, data)
            if generator.external_id in existing:
                raise ValueError(f"Generator with external id {generator.external_id} already exists")
            assets_by_xid[generator.external_id] = generator

        for name, data in shop_model["model"]["plant"].items():
            plant = self._to_plant(name, data)
            if plant.external_id in existing:
                raise ValueError(f"Plant with external id {plant.external_id} already exists")
            assets_by_xid[plant.external_id] = plant

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

        startcost_ts: dict[str, float] = data["startcost"]
        startcost = next(iter(startcost_ts.values()))

        generator_timeseries = self.times_series_by_generator_name.get(name, {})

        return GeneratorWrite(
            external_id=f"generator_{name}",
            name=name,
            ordering=None,
            p_min=data["p_min"],
            penstock=data["penstock"],
            start_cost=startcost,
            start_stop_cost=generator_timeseries.get("start_stop_cost"),
            is_available_time_series=generator_timeseries.get("start_stop_cost"),
            efficiency_curve=efficiency_curve,
            turbine_curves=turbine_curves,
        )

    def _to_plant(self, name: str, data: dict) -> PlantWrite:

        plant_timeseries = self.times_series_by_plant_name.get(name, {})

        return PlantWrite(
            external_id=f"plant_{name}",
            name=name,
            display_name=None,
            outlet_level=float(data.get("outlet_line", 0)),
            p_min=float(data.get("p_min", self.p_min_fallback)),
            p_max=float(data.get("p_max", self.p_max_fallback)),
            ordering=None,
            penstock_head_loss_factors={
                str(penstock_number): float(loss_factor)
                for penstock_number, loss_factor in enumerate(
                    data.get("penstock_loss", [self.head_loss_factor_fallback]), start=1
                )
            },
            head_loss_factor=float(data.get("main_loss", [self.head_loss_factor_fallback])[0]),
            # Todo Move over calculation of connection_losses
            connection_losses=None,
            water_value_time_series=plant_timeseries.get("water_value"),
            inlet_level_time_series=plant_timeseries.get("inlet_reservoir_level"),
            outlet_level_time_series=plant_timeseries.get("outlet_reservoir_level"),
            p_min_time_series=plant_timeseries.get("p_min"),
            p_max_time_series=plant_timeseries.get("p_max"),
            feeding_fee_time_series=plant_timeseries.get("feeding_fee"),
            head_direct_time_series=plant_timeseries.get("head_direct"),
        )
