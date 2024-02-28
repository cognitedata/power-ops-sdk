#!/usr/bin/env python
# Mypy does not understand the pydantic classes that allows both alias and name to be used in population
# type: ignore # noqa: [call-arg]
from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from cognite.client.data_classes import TimeSeries

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelWrite,
    GeneratorEfficiencyCurveWrite,
    GeneratorWrite,
    TurbineEfficiencyCurveWrite,
)
from cognite.powerops.utils.serialization import load_yaml


class PowerAssetImporter:
    def __init__(self, shop_models: list[dict], generator_times_series_mappings: Optional[list[dict]] = None):
        self.shop_models = shop_models
        self.times_series_by_generator_name = {
            entry["generator_name"]: entry for entry in generator_times_series_mappings or []
        }

    @classmethod
    def from_directory(cls, directory: Path, file_name: str = "model_raw") -> PowerAssetImporter:
        shop_model_files = list(directory.glob(f"**/{file_name}.yaml"))
        shop_models = [load_yaml(file) for file in shop_model_files]

        generator_mapping_file = directory / "generator_time_series_mappings.yaml"
        generator_mappings = load_yaml(generator_mapping_file) if generator_mapping_file.exists() else None

        return cls(shop_models, generator_mappings)

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

        is_available = generator_timeseries.get("is_available")
        start_stop_cost = generator_timeseries.get("start_stop_cost")
        return GeneratorWrite(
            external_id=f"generator_{name}",
            name=name,
            ordering=None,
            p_min=data["p_min"],
            penstock=data["penstock"],
            start_cost=startcost,
            start_stop_cost=TimeSeries(external_id=start_stop_cost) if start_stop_cost else None,
            is_available_time_series=TimeSeries(external_id=is_available) if is_available else None,
            efficiency_curve=efficiency_curve,
            turbine_curves=turbine_curves,
        )
