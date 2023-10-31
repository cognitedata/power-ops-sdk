from __future__ import annotations

import re

from cognite.powerops.client.data_classes import (
    GeneratorApply,
    PlantApply,
    PriceAreaApply,
    ReservoirApply,
    WatercourseApply,
    WatercourseShopApply,
)
from cognite.powerops.resync import config
from cognite.powerops.resync.models._shared_v1_v2._to_instances import make_ext_id
from cognite.powerops.resync.models._shared_v1_v2.production_model import (
    _create_generator_efficiency_curve,
    _create_turbine_efficiency_curve,
    _get_single_value,
    _plant_to_inlet_reservoir_with_losses,
    head_loss_factor_fallback,
    p_max_fallback,
    p_min_fallback,
)
from cognite.powerops.resync.models.v2.production_dm import ProductionModelDM


def to_production_data_model(configuration: config.ProductionConfig) -> ProductionModelDM:
    model = ProductionModelDM()
    plant_time_series_mappings_by_name = {
        mapping.plant_name: mapping for mapping in (configuration.plant_time_series_mappings or [])
    }
    start_stop_cost_time_series_by_generator = {
        mapping.generator_name: mapping.start_stop_cost
        for mapping in (configuration.generator_time_series_mappings or [])
    }
    is_generator_available = {
        mapping.generator_name: mapping.is_available for mapping in (configuration.generator_time_series_mappings or [])
    }

    for watercourse_config in configuration.watercourses:
        watercourse = WatercourseApply(
            external_id=f"watercourse_{watercourse_config.name}",
            name=watercourse_config.name,
            shop=WatercourseShopApply(
                external_id=make_ext_id(watercourse_config.shop_penalty_limit, WatercourseShopApply),
                penalty_limit=watercourse_config.shop_penalty_limit,
            ),
            plants=[],
            production_obligation_time_series=watercourse_config.production_obligation_ts_ext_ids,
        )
        model.watercourses.append(watercourse)

        shop_case = watercourse_config.shop_model_template

        reservoirs = []
        for reservoir_name in shop_case["model"]["reservoir"]:
            reservoir = ReservoirApply(
                external_id=f"reservoir_{reservoir_name}",
                name=reservoir_name,
                **dict(
                    zip(
                        ["display_name", "ordering"],
                        watercourse_config.reservoir_display_names_and_order.get(
                            reservoir_name, (re.sub(r"\([0-9]+\)", "", reservoir_name), "999")
                        ),
                    )
                ),
            )
            reservoirs.append(reservoir)
        model.reservoirs.extend(reservoirs)

        for generator_name, generator_attributes in shop_case["model"]["generator"].items():
            start_stop_cost = start_stop_cost_time_series_by_generator.get(generator_name)
            generator = GeneratorApply(
                external_id=f"generator_{generator_name}",
                name=generator_name,
                penstock=int(generator_attributes.get("penstock", 1)),
                p_min=float(generator_attributes.get("p_min", 0.0)),
                startcost=float(_get_single_value(generator_attributes.get("startcost", 0.0))),
                start_stop_cost=start_stop_cost,
                is_available_time_series=is_generator_available.get(generator_name),
            )
            efficiency_curve = _create_generator_efficiency_curve(
                generator_attributes, generator.name, generator.external_id
            )
            model.cdf_sequences.append(efficiency_curve)
            generator.generator_efficiency_curve = efficiency_curve.external_id
            turbine_efficiency_curve = _create_turbine_efficiency_curve(
                generator_attributes, generator.name, generator.external_id
            )

            generator.turbine_efficiency_curve = turbine_efficiency_curve.external_id
            model.generators.append(generator)
            model.cdf_sequences.append(turbine_efficiency_curve)

        generators_by_name = {generator.name: generator for generator in model.generators}
        plants = []
        for plant_name, attributes in shop_case["model"]["plant"].items():
            display_name, order = watercourse_config.plant_display_names_and_order.get(
                plant_name, (re.sub(r"\([0-9]+\)", "", plant_name), "999")
            )
            mapping = plant_time_series_mappings_by_name.get(plant_name)
            if mapping:
                mappings = dict(
                    water_value_time_series=mapping.water_value,
                    inlet_level_time_series=mapping.inlet_reservoir_level,
                    outlet_level_time_series=mapping.outlet_reservoir_level,
                    feeding_fee_time_series=mapping.feeding_fee,
                    p_min_time_series=mapping.p_min,
                    p_max_time_series=mapping.p_max,
                    head_direct_time_series=mapping.head_direct,
                )
            else:
                mappings = {}

            all_connections = shop_case["connections"]
            all_junctions = shop_case["model"].get("junction", {})
            all_tunnels = shop_case["model"].get("tunnel", {})
            inlet_reservoir_name, connection_losses = _plant_to_inlet_reservoir_with_losses(
                plant_name, all_connections, all_junctions, all_tunnels, {r.name for r in model.reservoirs}
            )

            # TODO: In next iteration of production data model,
            # we will have to add field connection losses and regenerate pygen sdk
            plant = PlantApply(
                external_id=f"plant_{plant_name}",
                name=plant_name,
                display_name=display_name,
                ordering=order,
                watercourse=watercourse,
                outlet_level=float(attributes.get("outlet_line", 0)),
                p_min=float(attributes.get("p_min", p_min_fallback)),
                p_max=float(attributes.get("p_max", p_max_fallback)),
                head_loss_factor=float(attributes.get("main_loss", [head_loss_factor_fallback])[0]),
                connection_losses=connection_losses,
                penstock_head_loss_factors={
                    str(penstock_number): float(loss_factor)
                    for penstock_number, loss_factor in enumerate(
                        attributes.get("penstock_loss", [head_loss_factor_fallback]), start=1
                    )
                },
                **mappings,
            )

            selected_reservoir = next((r for r in model.reservoirs if r.name == inlet_reservoir_name), None)
            if selected_reservoir is not None:
                plant.inlet_reservoir = selected_reservoir
            else:
                # Todo Raise Exception?
                ...
            parsed_connections = [config.Connection(**connection) for connection in all_connections]
            # Add the generators to the plant
            plant.generators = [
                g
                for connection in parsed_connections
                if (g := connection.to_from_any(generators_by_name, plant.name, "plant"))
            ] + [
                g
                for connection in parsed_connections
                if (g := connection.from_to_any(plant.name, "plant", generators_by_name))
            ]
            plants.append(plant)

            prod_area = str(next(iter(attributes["prod_area"].values())))
            price_area_name = watercourse_config.market_to_price_area[prod_area]
            price_area = PriceAreaApply(name=price_area_name, external_id=f"price_area_{price_area_name}")
            if price_area_name not in {a.name for a in model.price_areas}:
                if price_area_name in configuration.dayahead_price_timeseries:
                    price_area.dayahead_price_time_series = configuration.dayahead_price_timeseries[price_area_name]
                model.price_areas.append(price_area)
            price_area = next(a for a in model.price_areas if a.name == price_area_name)

            if watercourse.plants is None:
                watercourse.plants = [plant]
            else:
                watercourse.plants.append(plant)
            if price_area.plants is None:
                price_area.plants = [plant]
            else:
                price_area.plants.append(plant)
            if watercourse.name not in {w.name for w in price_area.watercourses or []}:
                if price_area.watercourses is None:
                    price_area.watercourses = [watercourse]
                else:
                    price_area.watercourses.append(watercourse)
        model.plants.extend(plants)

    return model
