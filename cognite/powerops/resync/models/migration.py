from cognite.client.data_classes import TimeSeries

from cognite.powerops.client.data_classes import PlantApply
from cognite.powerops.resync.models.v1 import Generator, Plant, PriceArea, ProductionModel, Reservoir, Watercourse
from cognite.powerops.resync.models.v1.production import WaterCourseShop
from cognite.powerops.resync.models.v2 import ProductionModelDM


def production_as_asset(dm: ProductionModelDM) -> ProductionModel:
    generator_efficiency_curve_by_external_id = {
        sequence.external_id: sequence
        for sequence in dm.cdf_sequences
        if sequence.external_id.endswith("_generator_efficiency_curve")
    }
    turbine_efficiency_curve_by_external_id = {
        sequence.external_id: sequence
        for sequence in dm.cdf_sequences
        if sequence.external_id.endswith("_turbine_efficiency_curve")
    }
    generators = []
    for generator in dm.generators:
        gen = Generator(
            name=generator.name,
            p_min=generator.p_min,
            penstock=str(generator.penstock),
            startcost=generator.startcost,
            start_stop_cost_time_series=generator.start_stop_cost and TimeSeries(external_id=generator.start_stop_cost),
            generator_efficiency_curve=generator_efficiency_curve_by_external_id.get(
                generator.generator_efficiency_curve
            ),
            turbine_efficiency_curve=turbine_efficiency_curve_by_external_id.get(generator.turbine_efficiency_curve),
            is_available_time_series=generator.is_available_time_series
            and TimeSeries(external_id=generator.is_available_time_series),
        )
        # External ID is automatically set in Asset classes, so need to manually
        # overwrite to check that the data class uses the same.
        gen.external_id = generator.external_id
        generators.append(gen)

    reservoirs = []
    for reservoir in dm.reservoirs:
        res = Reservoir(name=reservoir.name, display_name=reservoir.display_name, ordering=str(reservoir.ordering))
        res.external_id = reservoir.external_id
        reservoirs.append(res)

    generators_by_external_id = {generator.external_id: generator for generator in generators}
    reservoirs_by_external_id = {reservoir.external_id: reservoir for reservoir in reservoirs}

    plants = []
    for plant in dm.plants:
        plant: PlantApply
        asset_plant = Plant(
            name=plant.name,
            display_name=plant.display_name,
            ordering=str(plant.ordering),
            head_loss_factor=plant.head_loss_factor,
            outlet_level=plant.outlet_level,
            p_min=plant.p_min,
            p_max=plant.p_max,
            penstock_head_loss_factors=plant.penstock_head_loss_factors,
            connection_losses=plant.connection_losses,
            generators=[generators_by_external_id.get(g) for g in plant.generators or []],
            inlet_reservoir=reservoirs_by_external_id.get(plant.inlet_reservoir),
            p_min_time_series=plant.p_min_time_series and TimeSeries(external_id=plant.p_min_time_series),
            p_max_time_series=plant.p_max_time_series and TimeSeries(external_id=plant.p_max_time_series),
            water_value_time_series=plant.water_value_time_series
            and TimeSeries(external_id=plant.water_value_time_series),
            feeding_fee_time_series=plant.feeding_fee_time_series
            and TimeSeries(external_id=plant.feeding_fee_time_series),
            outlet_level_time_series=plant.outlet_level_time_series
            and TimeSeries(external_id=plant.outlet_level_time_series),
            inlet_level_time_series=plant.inlet_level_time_series
            and TimeSeries(external_id=plant.inlet_level_time_series),
            head_direct_time_series=plant.head_direct_time_series
            and TimeSeries(external_id=plant.head_direct_time_series),
        )
        asset_plant.external_id = plant.external_id
        plants.append(asset_plant)

    plants_by_external_id = {plant.external_id: plant for plant in plants}

    watercourses = []
    for watercourse in dm.watercourses:
        water = Watercourse(
            name=watercourse.name,
            shop=WaterCourseShop(penalty_limit=str(watercourse.shop.penalty_limit)),
            plants=[plants_by_external_id.get(p) for p in watercourse.plants],
            production_obligation_time_series=[
                TimeSeries(external_id=wa) for wa in watercourse.production_obligation_time_series if wa is not None
            ],
        )
        water.external_id = watercourse.external_id
        watercourses.append(water)

    watercourses_by_external_id = {watercourse.external_id: watercourse for watercourse in watercourses}

    price_areas = []
    for price_area in dm.price_areas:
        price_area_asset = PriceArea(
            name=price_area.name,
            watercourses=[watercourses_by_external_id.get(w) for w in price_area.watercourses],
            plants=[plants_by_external_id.get(p) for p in price_area.plants],
            dayahead_price_time_series=price_area.dayahead_price_time_series
            and TimeSeries(external_id=price_area.dayahead_price_time_series),
        )
        price_area_asset.external_id = price_area.external_id
        price_areas.append(price_area_asset)

    return ProductionModel(
        plants=plants, generators=generators, reservoirs=reservoirs, watercourses=watercourses, price_areas=price_areas
    )
