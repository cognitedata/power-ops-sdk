from __future__ import annotations

import itertools
import json
import logging

import pandas as pd
import yaml
from cognite.client.data_classes import Sequence

from cognite.powerops.client.data_classes import cogshop1 as cogshop_v1
from cognite.powerops.resync import config
from cognite.powerops.resync.models._shared_v1_v2.cogshop_model import (
    _create_transformation,
    _to_shop_files,
    _to_shop_model_file,
)
from cognite.powerops.resync.models.base import CDFSequence
from cognite.powerops.resync.models.v1.cogshop import CogShop1Asset
from cognite.powerops.resync.models.v1.market import DayAheadProcess, RKOMProcess
from cognite.powerops.resync.models.v1.production import Watercourse

logger = logging.getLogger(__name__)


def to_cogshop_asset_model(
    configuration: config.CogShopConfig,
    watercourses: list[Watercourse],
    shop_version: str,
    dayahead_processes: list[DayAheadProcess],
    rkom_processes: list[RKOMProcess],
) -> CogShop1Asset:
    model = CogShop1Asset()

    model.shop_files.extend(_to_shop_files(configuration.watercourses_shop))

    # TODO Fix the assumption that timeseries mappings and watercourses are in the same order
    for watercourse, mapping in zip(watercourses, configuration.time_series_mappings):
        model_file = _to_shop_model_file(
            watercourse.name,
            watercourse.model_file,
            watercourse.processed_model_file,
            watercourse.write_back_model_file,
        )
        model.shop_files.append(model_file)

        ##### Output definition #####
        external_id = f"SHOP_{watercourse.name.replace(' ', '_')}_output_definition"

        sequence = Sequence(
            name=external_id.replace("_", " "),
            description="Defining which SHOP results to output to CDF (as time series)",
            external_id=external_id,
            columns=[
                {"valueType": "STRING", "externalId": "shop_object_type"},
                {"valueType": "STRING", "externalId": "shop_attribute_name"},
                {"valueType": "STRING", "externalId": "cdf_attribute_name"},
                {"valueType": "STRING", "externalId": "unit"},
                {"valueType": "STRING", "externalId": "is_step"},
            ],
            metadata={"shop:watercourse": watercourse.name, "shop:type": "output_definition"},
        )
        # Only default mapping is used
        df = pd.DataFrame(
            [
                ("market", "sale_price", "price", "EUR/MWh", "True"),
                ("market", "sale", "sales", "MWh", "True"),
                ("plant", "production", "production", "MW", "True"),
                ("plant", "consumption", "consumption", "MW", "True"),
                ("reservoir", "water_value_global_result", "water_value", "EUR/Mm3", "True"),
                ("reservoir", "energy_conversion_factor", "energy_conversion_factor", "MWh/Mm3", "True"),
            ],
            columns=[c.external_id for c in sequence.columns],
        )

        output_definition = CDFSequence(sequence=sequence, content=df)

        model.output_definitions.append(output_definition)

        ##### Base Mapping #####
        external_id = f"SHOP_{watercourse.name}_base_mapping"
        sequence = Sequence(
            name=external_id.replace("_", " "),
            external_id=external_id,
            description="Mapping between SHOP paths and CDF TimeSeries",
            columns=mapping.column_definitions,
            metadata={"shop:watercourse": watercourse.name, "shop:type": "base_mapping"},
        )
        output_definition = CDFSequence(sequence=sequence, content=mapping.to_dataframe())
        model.base_mappings.append(output_definition)

        ### Model Template ###
        model_template = cogshop_v1.ModelTemplateApply(
            external_id=f"ModelTemplate_{watercourse.name}",
            version="1",
            shop_version=shop_version,
            watercourse=watercourse.name,
            source="resync",
            model=cogshop_v1.FileRefApply(
                external_id=f"ModelTemplate_{watercourse.name}__FileRef_model",
                type="case",
                file_external_id=model_file.external_id,
            ),
            base_mappings=[
                cogshop_v1.MappingApply(
                    external_id=f"BM_{watercourse.name}_{row.shop_model_path}",
                    path=row.shop_model_path,
                    timeseries_external_id=row.time_series_external_id,
                    transformations=[
                        _create_transformation(order, transformation)
                        for order, transformation in enumerate(row.transformations or [])
                    ],
                    retrieve=row.retrieve.name if row.retrieve else None,
                    aggregation=row.aggregation.name if row.aggregation else None,
                )
                for row in mapping
            ],
        )
        model.model_templates[model_template.external_id] = model_template

    model.mappings.update(
        {
            mapping.external_id: mapping
            for template in model.model_templates.values()
            for mapping in template.base_mappings
        }
    )
    # TODO: extend here to use adapter to translate to new transformations and extend the model with those
    model.transformations.update(
        {
            t.external_id: t
            for template in model.model_templates.values()
            for mapping in template.base_mappings
            for t in mapping.transformations
        }
    )

    command_file_by_watercourse = {
        f.meta.metadata["shop:watercourse"]: f
        for f in model.shop_files
        if f.meta.metadata.get("shop:type") == "commands"
    }

    for process in itertools.chain(dayahead_processes, rkom_processes):
        for incremental_mapping in process.incremental_mapping:
            incremental_mapping: CDFSequence
            watercourse = incremental_mapping.sequence.metadata["shop:watercourse"]
            scenario_name = incremental_mapping.sequence.metadata["bid:scenario_name"]
            external_id = f"Scenario_{watercourse}_{scenario_name}"
            command_file = command_file_by_watercourse.get(watercourse)

            if command_file is None:
                raise ValueError(f"Could not find commands file for watercourse {watercourse}")
            scenario = cogshop_v1.ScenarioApply(
                external_id=external_id,
                name=f"Scenario {watercourse} {scenario_name}",
                model_template=model.model_templates[f"ModelTemplate_{watercourse}"].external_id,
                mappings_override=[
                    cogshop_v1.MappingApply(
                        external_id=f"Mapping_{external_id}_{i}",
                        path=mapping["shop_model_path"],
                        timeseries_external_id=mapping.get("time_series_external_id"),
                        transformations=[
                            _create_transformation(j, transformation_data)
                            for j, transformation_data in enumerate(json.loads(mapping.get("transformations", "")))
                        ],
                        retrieve=mapping.get("retrieve"),
                        aggregation=mapping.get("aggregation"),
                    )
                    for i, mapping in enumerate(
                        incremental_mapping.content.replace(float("nan"), None).to_dict(orient="records")
                    )
                ],
                commands=cogshop_v1.CommandsConfigApply(
                    external_id=f"Commands_{watercourse}",
                    commands=yaml.safe_load(command_file.content.decode()).get("commands", []),
                ),
                source="resync",
            )
            model.scenarios[scenario.external_id] = scenario

    model.mappings.update(
        {
            mapping.external_id: mapping
            for scenario in model.scenarios.values()
            for mapping in scenario.mappings_override
        }
    )
    model.transformations.update(
        {
            t.external_id: t
            for scenario in model.scenarios.values()
            for mapping in scenario.mappings_override
            for t in mapping.transformations
        }
    )
    model.commands_configs.update(
        {scenario.commands.external_id: scenario.commands for scenario in model.scenarios.values()}
    )

    return model
