from __future__ import annotations

import ast
import json
import os
import typing
from collections import defaultdict
from pathlib import Path
from typing import ClassVar, Dict, Generator, List, Optional, Tuple

from cognite.client.data_classes import Asset, Label, Sequence
from pydantic import BaseModel, Field, root_validator, validator

from cognite.powerops.data_classes.benchmarking_config import BenchmarkingConfig
from cognite.powerops.data_classes.cdf_resource_collection import (
    BootstrapResourceCollection,
    SequenceContent,
    SequenceRows,
)
from cognite.powerops.data_classes.common import AggregationMethod, CommonConstants, RelativeTime, RetrievalType
from cognite.powerops.data_classes.config_model import Configuration
from cognite.powerops.data_classes.market_config import MarketConfig
from cognite.powerops.data_classes.reserve_scenario import Auction, Block, Product, ReserveScenario
from cognite.powerops.data_classes.rkom_bid_combination_config import RKOMBidCombinationConfig
from cognite.powerops.data_classes.rkom_market_config import RkomMarketConfig
from cognite.powerops.data_classes.time_series_mapping import (
    TimeSeriesMapping,
    TimeSeriesMappingEntry,
    write_mapping_to_sequence,
)
from cognite.powerops.data_classes.transformation import Transformation, TransformationType
from cognite.powerops.utils.cdf_utils import simple_relationship
from cognite.powerops.utils.common import print_warning
from cognite.powerops.utils.labels import AssetLabels, RelationshipLabels
from cognite.powerops.utils.serializer import load_yaml


class Watercourse(BaseModel):
    name: str
    shop_penalty_limit: int = 42000


class WatercourseConfig(Watercourse):
    version: str
    market_to_price_area: Dict[str, str]
    directory: str
    model_raw: str
    # ------------------------------------------------------------------
    yaml_raw_path: str = ""
    yaml_processed_path: str = ""  # TODO: not used here
    yaml_mapping_path: str = ""
    model_processed: str  # TODO: not used here
    model_mapping: Optional[str] = None
    tco_paths: Optional[List[str]] = None  # TODO: not used here - HEV specific
    rrs_ids: Optional[List[str]] = None
    hardcoded_mapping: Optional[TimeSeriesMapping] = None  # TODO: not used here
    hist_flow_timeseries: Optional[Dict[str, str]] = None  # TODO: not used here
    # ------------------------------------------------------------------
    production_obligation_ts_ext_ids: Optional[List[str]] = None
    plant_display_names_and_order: Optional[Dict[str, tuple[str, int]]] = None
    reservoir_display_names_and_order: Optional[Dict[str, tuple[str, int]]] = None
    water_value_based_method_time_series_csv_filename: Optional[str] = None

    @classmethod
    def from_yaml(cls, yaml_path: Path) -> list["WatercourseConfig"]:
        return [cls(**watercourse_raw) for watercourse_raw in load_yaml(yaml_path)]

    def load_plants_by_price_area(self, path: Path) -> dict[str, list]:
        content = load_yaml(path / self.directory / self.model_raw, clean_data=True)
        plants_by_prod_area = defaultdict(list)
        for plant_name, plant_attributes in content["model"]["plant"].items():
            prod_area = str(list(plant_attributes["prod_area"].values())[0])
            price_area = self.market_to_price_area[prod_area]
            plants_by_prod_area[price_area].append(plant_name)
        return plants_by_prod_area

    def plant_display_name(self, plant: str) -> Optional[str]:
        try:
            return self.plant_display_names_and_order[plant][0]  # type: ignore
        except (KeyError, TypeError):
            print(f"[WARNING] No ordering key for plant: {plant}")
            return None

    def plant_ordering_key(self, plant: str) -> Optional[int]:
        try:
            return self.plant_display_names_and_order[plant][1]  # type: ignore
        except (KeyError, TypeError):
            print(f"[WARNING] No ordering key for plant: {plant}")
            return None

    def reservoir_display_name(self, reservoir: str) -> Optional[str]:
        try:
            return self.reservoir_display_names_and_order[reservoir][0]  # type: ignore
        except (KeyError, TypeError):
            print(f"[WARNING] No ordering key for plant: {reservoir}")
            return None

    def reservoir_ordering_key(self, reservoir: str) -> Optional[int]:
        try:
            return self.reservoir_display_names_and_order[reservoir][1]  # type: ignore
        except (KeyError, TypeError):
            print(f"[WARNING] No ordering key for plant: {reservoir}")
            return None

    def set_shop_yaml_paths(self, path):
        self.yaml_raw_path = f"{path}/{self.directory}/{self.model_raw}"  # TODO: create these as properties
        self.yaml_processed_path = f"{path}/{self.directory}/{self.model_processed}"
        self.yaml_mapping_path = f"{path}/{self.directory}/{self.model_mapping}"


class PriceScenarioID(BaseModel):
    id: str
    rename: str = ""


class PriceScenario(BaseModel):
    name: str
    time_series_external_id: Optional[str]
    transformations: Optional[List[Transformation]]

    def to_time_series_mapping(self) -> TimeSeriesMapping:
        retrieve = RetrievalType.RANGE if self.time_series_external_id else None
        transformations = self.transformations or []

        # to make buy price slightly higher than sale price in SHOP
        transformations_buy_price = transformations + [
            Transformation(transformation=TransformationType.ADD, kwargs={"value": 0.01})
        ]

        sale_price_row = TimeSeriesMappingEntry(
            object_type="market",
            object_name=self.name,
            attribute_name="sale_price",
            time_series_external_id=self.time_series_external_id,
            transformations=transformations,
            retrieve=retrieve,
            aggregation=AggregationMethod.mean,
        )

        buy_price_row = TimeSeriesMappingEntry(
            object_type="market",
            object_name=self.name,
            attribute_name="buy_price",
            time_series_external_id=self.time_series_external_id,
            transformations=transformations_buy_price,
            retrieve=retrieve,
            aggregation=AggregationMethod.mean,
        )

        return TimeSeriesMapping(rows=[sale_price_row, buy_price_row])


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
    ) -> BootstrapResourceCollection:
        bootstrap_resource_collection = BootstrapResourceCollection()

        sequence = self.get_sequence(price_area, bid_process_config_name)
        bootstrap_resource_collection.add(sequence)

        sequence_rows = self.to_sequence_rows(plant_names)
        bootstrap_resource_collection.add(
            SequenceContent(sequence_external_id=sequence.external_id, data=sequence_rows)
        )

        relationship = simple_relationship(
            source=bid_process_config_asset,
            target=sequence,
            label_external_id=RelationshipLabels.BID_MATRIX_GENERATOR_CONFIG_SEQUENCE,
        )
        bootstrap_resource_collection.add(relationship)

        return bootstrap_resource_collection


class BidProcessConfig(Configuration):
    name: str
    price_area_name: str = Field(alias="bid_price_area")
    price_scenarios: List[PriceScenarioID] = Field(alias="bid_price_scenarios")
    main_scenario: str = Field(alias="bid_main_scenario")
    bid_date: Optional[RelativeTime] = None
    shop_start: Optional[RelativeTime] = Field(None, alias="shop_starttime")
    shop_end: Optional[RelativeTime] = Field(None, alias="shop_endtime")
    bid_matrix_generator: str = Field(alias="bid_bid_matrix_generator_config_external_id")
    price_scenarios_per_watercourse: Optional[Dict[str, typing.Set[str]]] = None

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
            }

        return Asset(
            external_id=f"POWEROPS_bid_process_configuration_{self.name}",
            name=f"Bid process configuration {self.name}",
            metadata=to_metadata(price_scenarios_by_id, benchmark),
            description="Bid process configuration defining how to run a bid matrix generation process",
            parent_external_id="bid_process_configurations",
            labels=[Label(AssetLabels.BID_PROCESS_CONFIGURATION)],
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
        bootstrap_resources: BootstrapResourceCollection,
        price_scenarios_by_id: dict[str, PriceScenario],
        bid_matrix_generator_configs: list[BidMatrixGeneratorConfig],
        watercourses: list[WatercourseConfig],
        benchmark: BenchmarkingConfig,
    ) -> BootstrapResourceCollection:
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
            and RelationshipLabels.WATERCOURSE in rel.labels[0].values()
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
        incremental_mapping_bootstrap_resources = BootstrapResourceCollection()
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
                label_external_id=RelationshipLabels.INCREMENTAL_MAPPING_SEQUENCE,
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
        new_bootstrap_resources = BootstrapResourceCollection()
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


# TODO: Put this in a configgen module?
def map_price_scenarios_by_name(
    scenario_ids: List[PriceScenarioID], price_scenarios_by_id: dict[str, PriceScenario], market_name: str
) -> dict[str, PriceScenario]:
    scenario_by_name = {}
    for identifier in scenario_ids:
        ref_scenario = price_scenarios_by_id[identifier.id]
        name = identifier.rename or ref_scenario.name or identifier.id
        scenario_by_name[name] = PriceScenario(name=market_name, **ref_scenario.dict(exclude={"name"}))
    return scenario_by_name


MARKET_BY_PRICE_AREA = {"NO2": "Dayahead", "NO1": "1", "NO3": "1", "NO5": "1"}


class CDFConfig(BaseModel):
    TENANT_ID: str
    CLIENT_ID: str
    CDF_CLUSTER: str
    COGNITE_PROJECT: str
    SPACE_ID: str
    DATA_MODEL: str
    SCHEMA_VERSION: str

    @classmethod
    def from_env(cls) -> "CDFConfig":
        return cls(
            **{
                "TENANT_ID": os.environ.get("TENANT_ID"),
                "CLIENT_ID": os.environ.get("CLIENT_ID"),
                "CLIENT_SECRET_ENV": os.environ.get("CLIENT_SECRET_ENV"),
                "CDF_CLUSTER": os.environ.get("CDF_CLUSTER"),
                "COGNITE_PROJECT": os.environ.get("COGNITE_PROJECT"),
                "SPACE_ID": os.environ.get("SPACE_ID"),
                "DATA_MODEL": os.environ.get("DATA_MODEL"),
                "SCHEMA_VERSION": os.environ.get("SCHEMA_VERSION"),
            }
        )


class ReserveScenarios(BaseModel):
    volumes: list[int]
    auction: Auction
    product: Product
    block: Block
    reserve_group: str
    mip_plant_time_series: List[Tuple[str, Optional[str]]]
    obligation_external_id: Optional[str]

    @validator("auction", pre=True)
    def to_enum(cls, value):
        return Auction[value] if isinstance(value, str) else value

    @validator("volumes", pre=True)
    def valid_volumes(cls, volumes):
        if 0 not in volumes:
            raise ValueError("You probably want 0 MW as one of the volumes!")
        if any(volume < 0 for volume in volumes):
            raise ValueError(f"All volumes should be positive! Got {volumes}")
        return list(set(volumes))  # Do not want duplicate volumes

    def __str__(self) -> str:
        return f'[{"MW, ".join(str(volume) for volume in sorted(self.volumes))}MW]'  # E.g "[0MW, 10MW, 20MW]"

    def __len__(self) -> int:
        return len(self.list_scenarios())

    def __iter__(self) -> Generator[ReserveScenario, None, None]:  # type: ignore
        yield from self.list_scenarios()

    def list_scenarios(self) -> List[ReserveScenario]:
        return [
            ReserveScenario(
                volume=volume,
                auction=self.auction,
                product=self.product,
                block=self.block,
                reserve_group=self.reserve_group,
                mip_plant_time_series=self.mip_plant_time_series,
                obligation_external_id=self.obligation_external_id,
            )
            for volume in self.volumes
        ]


class RKOMBidProcessConfig(Configuration):
    watercourse: str = Field(alias="bid_watercourse")

    price_scenarios: List[PriceScenarioID] = Field(alias="bid_price_scenarios")
    reserve_scenarios: ReserveScenarios = Field(alias="bid_reserve_scenarios")

    shop_start: RelativeTime = Field(alias="shop_starttime")
    shop_end: RelativeTime = Field(alias="shop_endtime")

    timezone: str = "Europe/Oslo"
    method: str = "simple"

    minimum_price: int = 0  # TODO: need to specify currency
    price_premium: int = 0  # TODO: need to specify currency

    parent_external_id: typing.ClassVar[str] = "rkom_bid_process_configurations"
    mapping_type: ClassVar[str] = "rkom_incremental_mapping"

    @root_validator(pre=True)
    def create_reserve_scenarios(cls, values):
        if not isinstance(volumes := values.get("bid_reserve_scenarios"), str):
            return values
        volumes = [int(volume.removesuffix("MW")) for volume in volumes[1:-1].split(",")]

        values["bid_reserve_scenarios"] = dict(
            volumes=volumes,
            auction=values["bid_auction"],
            product=values["bid_product"],
            block=values["bid_block"],
            reserve_group=values["labels"][0]["externalId"],
            mip_plant_time_series=[],
        )
        return values

    @validator("shop_start", "shop_end", pre=True)
    def json_loads(cls, value):
        return {"operations": json.loads(value)} if isinstance(value, str) else value

    @validator("price_scenarios", pre=True)
    def literal_eval(cls, value):
        return [{"id": id_} for id_ in ast.literal_eval(value)] if isinstance(value, str) else value

    @property
    def sorted_volumes(self) -> List[int]:
        return sorted(self.reserve_scenarios.volumes)

    @property
    def name(self) -> str:
        return (
            f"{self.watercourse}_"
            f"{self.reserve_scenarios.auction.value}_"
            f"{self.reserve_scenarios.product}_"
            f"{self.reserve_scenarios.block}_"
            f"{len(self.price_scenarios)}-prices_"
            f"{self.sorted_volumes[1]}MW-{self.sorted_volumes[-1]}MW"
        )

    @property
    def external_id(self) -> str:
        return f"POWEROPS_{self.name}"

    @property
    def bid_date(self) -> RelativeTime:
        if self.reserve_scenarios.auction == "week":
            return RelativeTime(relative_time_string="monday")
        else:
            return RelativeTime(relative_time_string="saturday")

    @property
    def rkom_plants(self) -> List[str]:
        return [plant for plant, _ in self.reserve_scenarios.mip_plant_time_series]

    def to_metadata(self, rkom_price_scenarios_by_id: dict[str, PriceScenario], rkom_market_name: str) -> dict:
        price_scenarios = map_price_scenarios_by_name(
            self.price_scenarios, rkom_price_scenarios_by_id, rkom_market_name
        )

        return {
            "bid:watercourse": self.watercourse,
            "bid:auction": self.reserve_scenarios.auction.value,
            "bid:block": self.reserve_scenarios.block,
            "bid:product": self.reserve_scenarios.product,
            "bid:method": self.method,
            "bid:date": str(self.bid_date),
            "bid:price_scenarios": str(list(price_scenarios)),
            "bid:reserve_scenarios": str(self.reserve_scenarios),
            "bid:minimum_price": str(self.minimum_price),
            "bid:price_premium": str(self.price_premium),
            "rkom:plants": str(sorted(self.rkom_plants)),
            "shop:starttime": str(self.shop_start),
            "shop:endtime": str(self.shop_end),
            "timezone": self.timezone,
        }

    def to_cdf_asset(self, rkom_price_scenarios_by_id: dict[str, PriceScenario], rkom_market_name: str) -> Asset:
        return Asset(
            external_id=self.external_id,
            name=self.name,
            metadata=self.to_metadata(rkom_price_scenarios_by_id, rkom_market_name),
            description=f"RKOM bid generation config for {self.watercourse}",
            parent_external_id=self.parent_external_id,
            labels=[Label(AssetLabels.RKOM_BID_CONFIGURATION)],
        )

    def to_bootstrap_resources(self, price_scenarios_by_id, market_name) -> BootstrapResourceCollection:
        bootstrap_resources = BootstrapResourceCollection()
        asset = self.to_cdf_asset(price_scenarios_by_id, market_name)
        bootstrap_resources.add(asset)

        price_scenarios = map_price_scenarios_by_name(self.price_scenarios, price_scenarios_by_id, market_name)

        # Create incremental mapping for each combination of price scenario and reserve_scenario
        for price_scenario_name, price_scenario in price_scenarios.items():
            price_mapping = price_scenario.to_time_series_mapping()
            for reserve_scenario in self.reserve_scenarios:
                reserve_mapping = reserve_scenario.to_time_series_mapping()

                bootstrap_resources += write_mapping_to_sequence(
                    watercourse=self.watercourse,
                    mapping=price_mapping + reserve_mapping,
                    mapping_type=self.mapping_type,  # type: ignore
                    reserve_volume=reserve_scenario.volume,
                    price_scenario_name=price_scenario_name,
                    config_name=self.name,
                )

        for sequence in bootstrap_resources.sequences.values():
            relationship = simple_relationship(
                source=asset,
                target=sequence,
                label_external_id=RelationshipLabels.INCREMENTAL_MAPPING_SEQUENCE,
            )
            bootstrap_resources.add(relationship)

        return bootstrap_resources


ExternalId = str


class PlantTimeSeriesMapping(BaseModel):
    plant_name: str
    water_value: Optional[ExternalId]
    inlet_reservoir_level: Optional[ExternalId]
    outlet_reservoir_level: Optional[ExternalId]
    p_min: Optional[ExternalId]
    p_max: Optional[ExternalId]
    feeding_fee: Optional[ExternalId]
    head_direct: Optional[ExternalId]


class GeneratorTimeSeriesMapping(BaseModel):
    generator_name: str
    start_stop_cost: Optional[ExternalId]


class BootstrapConfig(BaseModel):
    constants: CommonConstants
    cdf: CDFConfig
    benchmarks: list[BenchmarkingConfig]
    price_scenario_by_id: dict[str, PriceScenario]
    bidprocess: list[BidProcessConfig]
    bidmatrix_generators: list[BidMatrixGeneratorConfig]
    dayahead_price_timeseries: Dict[str, str]
    market: MarketConfig
    watercourses: list[WatercourseConfig]
    time_series_mappings: list[TimeSeriesMapping]
    rkom_bid_combination: Optional[list[RKOMBidCombinationConfig]] = None
    rkom_bid_process: list[RKOMBidProcessConfig]
    rkom_market: Optional[RkomMarketConfig] = None
    plant_time_series_mappings: list[PlantTimeSeriesMapping] = None
    generator_time_series_mappings: list[GeneratorTimeSeriesMapping] = None

    @validator("price_scenario_by_id")
    def no_duplicated_scenarios(cls, value: dict[str, PriceScenario]):
        scenarios_by_hash = defaultdict(list)
        for id_, s in value.items():
            scenarios_by_hash[hash(s.json(exclude={"name"}))].append((id_, s))
        if duplicated := [s for s in scenarios_by_hash.values() if len(s) > 1]:
            sep = ") |\n\t\t\t ("
            raise ValueError(
                f"Scenarios  "
                f"({sep.join([', '.join([id_ for id_, _ in duplicate_set]) for duplicate_set in duplicated])}) "
                f"\nare duplicated."
            )
        return value

    @classmethod
    def from_yamls(cls, config_dir_path: Path) -> "BootstrapConfig":
        configs = {}
        for field_name in cls.__fields__:
            if (config_file_path := config_dir_path / f"{field_name}.yaml").exists():
                configs[field_name] = load_yaml(config_file_path, encoding="utf-8")
        return cls(**configs)
