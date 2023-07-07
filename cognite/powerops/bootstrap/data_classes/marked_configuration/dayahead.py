from __future__ import annotations

import ast
import json
import typing
from collections import defaultdict
from pathlib import Path
from typing import ClassVar, Dict, List, Optional

from cognite.client.data_classes import Asset, Label, Sequence
from pydantic import BaseModel, ConfigDict, Field, validator

from cognite.powerops.bootstrap.data_classes.bootstrap_resource_collection import (
    ResourceCollection,
    write_mapping_to_sequence,
)
from cognite.powerops.bootstrap.data_classes.cdf_labels import AssetLabels, RelationshipLabels
from cognite.powerops.bootstrap.data_classes.core.watercourse import WatercourseConfig
from cognite.powerops.bootstrap.data_classes.marked_configuration import (
    BenchmarkingConfig,
    PriceScenario,
    PriceScenarioID,
)
from cognite.powerops.bootstrap.data_classes.marked_configuration._core import (
    Configuration,
    RelativeTime,
    map_price_scenarios_by_name,
)
from cognite.powerops.bootstrap.data_classes.marked_configuration.market import MARKET_BY_PRICE_AREA
from cognite.powerops.bootstrap.data_classes.to_delete import SequenceContent, SequenceRows
from cognite.powerops.bootstrap.utils.common import print_warning
from cognite.powerops.utils.cdf.resource_creation import simple_relationship


class BidMatrixGeneratorConfig(BaseModel):
    name: str
    default_method: str
    default_function_external_id: str
    column_external_ids: ClassVar[list[str]] = [
        "shop_plant",
        "bid_matrix_generation_method",
        "function_external_id",
    ]

    @staticmethod
    def _metadata(price_area: str, bid_process_config_name: str) -> dict:
        # TODO: Rename this from "shop:type" to something without "shop"
        #   (but check if e.g. power-ops-functions uses this)
        return {
            "bid:price_area": f"price_area_{price_area}",
            "shop:type": "bid_matrix_generator_config",
            "bid:bid_process_configuration_name": bid_process_config_name,
        }

    def get_sequence(self, price_area: str, bid_process_config_name: str) -> Sequence:
        column_def = [{"valueType": "STRING", "externalId": external_id} for external_id in self.column_external_ids]
        return Sequence(
            external_id=f"POWEROPS_bid_matrix_generator_config_{bid_process_config_name}",
            name=f"POWEROPS bid matrix generator config {bid_process_config_name}",
            description="Configuration of bid matrix generation method to use for each plant in the price area",
            columns=column_def,
            metadata=self._metadata(price_area, bid_process_config_name),
        )

    def to_sequence_rows(self, plant_names: list[str]) -> SequenceRows:
        return SequenceRows(
            rows=[
                (idx, [plant, self.default_method, self.default_function_external_id])
                for idx, plant in enumerate(plant_names)
            ],
            columns_external_ids=self.column_external_ids,
        )

    def to_bootstrap_resources(
        self,
        bid_process_config_asset: Asset,
        price_area: str,
        bid_process_config_name: str,
        plant_names: list[str],
    ) -> ResourceCollection:
        bootstrap_resource_collection = ResourceCollection()

        sequence = self.get_sequence(price_area, bid_process_config_name)
        bootstrap_resource_collection.add(sequence)

        sequence_rows = self.to_sequence_rows(plant_names)
        bootstrap_resource_collection.add(
            SequenceContent(sequence_external_id=sequence.external_id, data=sequence_rows)
        )

        relationship = simple_relationship(
            source=bid_process_config_asset,
            target=sequence,
            label_external_id=RelationshipLabels.BID_MATRIX_GENERATOR_CONFIG_SEQUENCE.value,
        )
        bootstrap_resource_collection.add(relationship)

        return bootstrap_resource_collection


class BidProcessConfig(Configuration):
    model_config = ConfigDict(populate_by_name=True)

    name: str
    price_area_name: str = Field(alias="bid_price_area")
    price_scenarios: List[PriceScenarioID] = Field(alias="bid_price_scenarios")
    main_scenario: str = Field(alias="bid_main_scenario")
    bid_date: Optional[RelativeTime] = None
    shop_start: Optional[RelativeTime] = Field(None, alias="shop_starttime")
    shop_end: Optional[RelativeTime] = Field(None, alias="shop_endtime")
    bid_matrix_generator: str = Field(alias="bid_bid_matrix_generator_config_external_id")
    price_scenarios_per_watercourse: Optional[Dict[str, typing.Set[str]]] = None
    is_default_config_for_price_area: bool = False
    no_shop: bool = Field(False, alias="no_shop")

    @validator("shop_start", "shop_end", "bid_date", pre=True)
    def json_loads(cls, value):
        return {"operations": json.loads(value)} if isinstance(value, str) else value

    @validator("price_scenarios", pre=True)
    def literal_eval(cls, value):
        return [{"id": id_} for id_ in ast.literal_eval(value)] if isinstance(value, str) else value

    def to_cdf_asset(
        self,
        benchmark: BenchmarkingConfig,
        price_scenarios_by_id: dict[str, PriceScenario],
    ) -> Asset:
        def to_metadata(price_scenarios_by_id: dict[str, PriceScenario], benchmark: BenchmarkingConfig) -> dict:
            price_scenarios = map_price_scenarios_by_name(
                self.price_scenarios, price_scenarios_by_id, MARKET_BY_PRICE_AREA[self.price_area_name]
            )

            price_scenarios_string = json.dumps(
                {scenario_name: scenario.time_series_external_id for scenario_name, scenario in price_scenarios.items()}
            )
            return {
                "bid:bid_process_configuration_name": self.name,
                "bid:bid_matrix_generator_config_external_id": f"POWEROPS_bid_matrix_generator_config_{self.name}",
                "bid:market_config_external_id": benchmark.market_config_external_id,
                "bid:price_scenarios": price_scenarios_string,
                "bid:main_scenario": self.main_scenario,
                "bid:date": str(self.bid_date or benchmark.bid_date),
                "shop:starttime": str(self.shop_start or benchmark.shop_start),
                "shop:endtime": str(self.shop_end or benchmark.shop_end),
                "bid:price_area": f"price_area_{self.price_area_name}",
                "bid:is_default_config_for_price_area": self.is_default_config_for_price_area,
                "bid:no_shop": self.no_shop,
            }

        return Asset(
            external_id=f"POWEROPS_bid_process_configuration_{self.name}",
            name=f"Bid process configuration {self.name}",
            metadata=to_metadata(price_scenarios_by_id, benchmark),
            description="Bid process configuration defining how to run a bid matrix generation process",
            parent_external_id="bid_process_configurations",
            labels=[Label(AssetLabels.BID_PROCESS_CONFIGURATION.value)],
        )

    def get_price_scenarios(
        self,
        price_scenarios: dict[str, PriceScenario],  # TODO: This must be price_scenarios_per_watercourse
        watercourse_name: Optional[str] = None,
    ) -> Dict[str, PriceScenario]:
        if watercourse_name and self.price_scenarios_per_watercourse:
            try:
                return {
                    scenario_name: price_scenarios[scenario_name]
                    for scenario_name in self.price_scenarios_per_watercourse[watercourse_name]
                }
            except KeyError as e:
                raise KeyError(
                    f"Watercourse {watercourse_name} not defined in price_scenarios_per_watercourse "
                    f"for BidProcessConfig {self.name}"
                ) from e
        return price_scenarios

    def to_bootstrap_resources(
        self,
        path: Path,
        bootstrap_resources: ResourceCollection,
        price_scenarios_by_id: dict[str, PriceScenario],
        bid_matrix_generator_configs: list[BidMatrixGeneratorConfig],
        watercourses: list[WatercourseConfig],
        benchmark: BenchmarkingConfig,
    ) -> ResourceCollection:
        asset = self.to_cdf_asset(benchmark, price_scenarios_by_id)
        price_scenario_by_name = map_price_scenarios_by_name(
            self.price_scenarios, price_scenarios_by_id, MARKET_BY_PRICE_AREA[self.price_area_name]
        )
        bid_matrix_generator_config_resources = self.create_bid_matrix_generator_config_resources(
            bid_matrix_generator_configs,
            asset,
            path,
            watercourses,
        )

        watercourse_names = [
            bootstrap_resources.assets[rel.target_external_id].name
            for rel in bootstrap_resources.relationships.values()
            if rel.source_external_id == f"price_area_{self.price_area_name}"
            and RelationshipLabels.WATERCOURSE.value in rel.labels[0].values()
        ]

        if not watercourse_names:
            print_warning(
                f"No related watercourses found for price area {self.price_area_name}! No 'incremental mapping' "
                f"Sequences to write!"
            )
            return bid_matrix_generator_config_resources

        return bid_matrix_generator_config_resources + self.create_incremental_mapping_resources(
            price_scenario_by_name, watercourse_names, asset
        )

    def create_incremental_mapping_resources(
        self,
        price_scenario_by_name: dict[str, PriceScenario],
        watercourse_names: list[str],
        asset: Asset,
    ):
        # Create TimeSeriesMapping (incremental mapping) for each price scenario and watercourse
        incremental_mapping_bootstrap_resources = ResourceCollection()
        for watercourse in watercourse_names:
            for scenario_name, price_scenario in self.get_price_scenarios(price_scenario_by_name, watercourse).items():
                time_series_mapping = price_scenario.to_time_series_mapping()

                incremental_mapping_bootstrap_resources += write_mapping_to_sequence(
                    watercourse=watercourse,
                    mapping=time_series_mapping,
                    mapping_type="incremental_mapping",
                    price_scenario_name=scenario_name,
                    config_name=self.name,
                )

        for sequence in incremental_mapping_bootstrap_resources.sequences.values():
            relationship = simple_relationship(
                source=asset,
                target=sequence,
                label_external_id=RelationshipLabels.INCREMENTAL_MAPPING_SEQUENCE.value,
            )
            incremental_mapping_bootstrap_resources.add(relationship)
        return incremental_mapping_bootstrap_resources

    def create_bid_matrix_generator_config_resources(
        self,
        bid_matrix_generator_configs: list[BidMatrixGeneratorConfig],
        asset: Asset,
        path: Path,
        watercourses: list[WatercourseConfig],
    ):
        new_bootstrap_resources = ResourceCollection()
        new_bootstrap_resources.add(asset)

        plants_by_price_area: dict[str, list] = defaultdict(list)
        for w in watercourses:
            sub_dict = w.load_plants_by_price_area(path)
            for k, v in sub_dict.items():
                plants_by_price_area[k].extend(v)

        generator_by_name = {g.name: g for g in bid_matrix_generator_configs}

        # Create BidMatrixGeneratorConfig Sequence and Relationship to BidProcessConfig Asset
        new_bootstrap_resources += generator_by_name[self.bid_matrix_generator].to_bootstrap_resources(
            bid_process_config_asset=asset,
            price_area=self.price_area_name,
            bid_process_config_name=self.name,
            plant_names=plants_by_price_area[self.price_area_name],
        )
        return new_bootstrap_resources
