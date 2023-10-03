from __future__ import annotations

from cognite.powerops.client._generated.data_classes import (
    OutputContainerApply,
    OutputMappingApply,
    ScenarioMappingApply,
    ScenarioTemplateApply,
    ValueTransformationApply,
)
from cognite.powerops.resync.config import CogShopConfig, WatercourseConfig
from cognite.powerops.resync.models._shared_v1_v2._to_instances import _to_input_timeseries_mapping, make_ext_id
from cognite.powerops.resync.models._shared_v1_v2.cogshop_model import _to_shop_files, _to_shop_model_file
from cognite.powerops.resync.models.v2.cogshop import CogShopDataModel


def to_cogshop_data_model(
    config: CogShopConfig, watercourse_configs: list[WatercourseConfig], shop_version: str
) -> CogShopDataModel:
    model = CogShopDataModel()

    model.shop_files.extend(_to_shop_files(config.watercourses_shop))

    # TODO Fix the assumption that timeseries mappings and watercourses are in the same order
    for watercourse, mapping in zip(watercourse_configs, config.time_series_mappings):
        model_file = _to_shop_model_file(
            watercourse.name,
            watercourse.yaml_raw_path,
            watercourse.yaml_processed_path,
            watercourse.write_back_model_file,
        )
        model.shop_files.append(model_file)

        ##### Output Definitions #####
        # Only default mapping is used
        output_definitions = [
            OutputMappingApply(
                external_id=make_ext_id(values, class_=OutputMappingApply),
                shop_object_type=values[0],
                shop_attribute_name=values[1],
                cdf_attribute_name=values[2],
                unit=values[3],
                is_step=values[4],
            )
            for values in [
                ("market", "sale_price", "price", "EUR/MWh", True),
                ("market", "sale", "sales", "MWh", True),
                ("plant", "production", "production", "MW", True),
                ("plant", "consumption", "consumption", "MW", True),
                ("reservoir", "water_value_global_result", "water_value", "EUR/Mm3", True),
                ("reservoir", "energy_conversion_factor", "energy_conversion_factor", "MWh/Mm3", True),
            ]
        ]
        external_id = f"SHOP_{watercourse.name.replace(' ', '_')}_output_definition"
        output_container = OutputContainerApply(
            external_id=external_id,
            name=external_id.replace("_", " "),
            watercourse=watercourse.name,
            shop_type="output_definition",
            mappings=output_definitions,
        )

        model.output_definitions[external_id] = output_container

        ##### Base Mapping #####
        base_mappings = []
        transformations = {}
        for entry in mapping:
            base_mapping = _to_input_timeseries_mapping(entry)
            base_mappings.append(base_mapping)

            transformations.update(
                {t.external_id: t for t in base_mapping.transformations if isinstance(t, ValueTransformationApply)}
            )
        scenario_mapping = ScenarioMappingApply(
            external_id=f"SHOP_{watercourse.name}_base_mapping",
            watercourse=watercourse.name,
            shop_type="base_mapping",
            mapping_override=base_mappings,
        )

        model.value_transformations.update(transformations)

        model.scenario_templates.append(
            ScenarioTemplateApply(
                external_id=f"{ScenarioTemplateApply.__name__.removesuffix('Apply')}_{watercourse.name}",
                model=model_file.external_id,
                shop_version=shop_version,
                template_version="1",
                output_definitions=output_container,
                shop_files=[f.external_id for f in model.shop_files],
                watercourse=watercourse.name,
                base_mapping=scenario_mapping,
            )
        )

    return model
