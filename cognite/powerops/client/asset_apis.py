from __future__ import annotations

from typing import Sequence, Type

from cognite.client import CogniteClient
from cognite.client._constants import LIST_LIMIT_DEFAULT
from cognite.client.data_classes import Asset
from pydantic import BaseModel

from cognite.powerops.config import (
    BenchmarkingConfig,
    BidProcessConfig,
    MarketConfig,
    RKOMBidCombinationConfig,
    RKOMBidProcessConfig,
    Watercourse,
)
from cognite.powerops.data_classes.asset_lists import (
    BenchmarkingList,
    BidProcessList,
    GeneratorList,
    MarketConfigsList,
    PlantList,
    PriceAreaList,
    ReservoirList,
    RKOMBidCombinationList,
    RKOMBidList,
    WatercourseList,
)
from cognite.powerops.data_classes.core import T_AssetResourceList
from cognite.powerops.data_classes.generator import Generator
from cognite.powerops.data_classes.plant import Plant
from cognite.powerops.data_classes.price_area import PriceArea
from cognite.powerops.data_classes.reservoirs import Reservoir


def unpack_asset(asset: Asset) -> dict:
    unpacked = asset.dump(camel_case=False)
    if "metadata" in unpacked:
        metadata = unpacked.pop("metadata")
        for key, value in metadata.items():
            unpacked[key.replace(":", "_")] = value

    return unpacked


class AssetAPI:
    def __init__(
        self,
        client: CogniteClient,
        read_dataset: str,
        write_dataset: str,
        parent_external_id: str,
        external_id_prefix: str,
        class_type: Type[BaseModel],
        class_list: Type[T_AssetResourceList],
    ):
        self._client = client
        self._external_id_prefix = external_id_prefix
        self._parent_external_id = parent_external_id
        self._read_dataset = read_dataset
        self._write_dataset = write_dataset
        self._class_type = class_type
        self._class_list = class_list

    def _to_list_type(self, assets: Sequence[Asset]) -> T_AssetResourceList:
        return self._class_list([self._class_type(**unpack_asset(asset)) for asset in assets])

    def list(self, limit: int = LIST_LIMIT_DEFAULT) -> T_AssetResourceList:
        assets = self._client.assets.list(
            parent_external_ids=[self._parent_external_id],
            data_set_external_ids=[self._read_dataset, self._write_dataset],
            limit=limit,
        )
        return self._to_list_type(assets)

    def retrieve(self, name: str | list[str], ignore_unknown_ids: bool = False) -> BaseModel | T_AssetResourceList:
        names = name if isinstance(name, list) else [name]
        assets = self._client.assets.retrieve_multiple(
            external_ids=[f"{self._external_id_prefix}{name}" for name in names], ignore_unknown_ids=ignore_unknown_ids
        )

        if len(assets) == 1:
            return self._class_type(**unpack_asset(assets[0]))
        return self._to_list_type(assets)


class WatercourseAPI(AssetAPI):
    """Manage watercourses. Changes are directly applied to CDF."""

    def __init__(self, client: CogniteClient, read_dataset: str, write_dataset: str):
        super().__init__(
            client, read_dataset, write_dataset, "watercourses", "watercourse_", Watercourse, WatercourseList
        )

    #
    # def copy(self, watercourse: Watercourse, name: str) -> Watercourse:
    #     """Create a copy of an existing watercourse, with a new name."""


class PlantAPI(AssetAPI):
    def __init__(self, client: CogniteClient, read_dataset: str, write_dataset: str):
        super().__init__(client, read_dataset, write_dataset, "plants", "plant_", Plant, PlantList)


class ReservoirsAPI(AssetAPI):
    def __init__(self, client: CogniteClient, read_dataset: str, write_dataset: str):
        super().__init__(client, read_dataset, write_dataset, "reservoirs", "reservoir_", Reservoir, ReservoirList)


class GeneratorsAPI(AssetAPI):
    def __init__(self, client: CogniteClient, read_dataset: str, write_dataset: str):
        super().__init__(client, read_dataset, write_dataset, "generators", "generator_", Generator, GeneratorList)


class PriceAreasAPI(AssetAPI):
    def __init__(self, client: CogniteClient, read_dataset: str, write_dataset: str):
        super().__init__(client, read_dataset, write_dataset, "price_areas", "price_area_", PriceArea, PriceAreaList)


class BidConfigurationsAPI(AssetAPI):
    def __init__(self, client: CogniteClient, read_dataset: str, write_dataset: str):
        super().__init__(
            client,
            read_dataset,
            write_dataset,
            "bid_process_configurations",
            "POWEROPS_bid_process_configuration_",
            BidProcessConfig,
            BidProcessList,
        )


class RKOMBidConfigurationsAPI(AssetAPI):
    def __init__(self, client: CogniteClient, read_dataset: str, write_dataset: str):
        super().__init__(
            client,
            read_dataset,
            write_dataset,
            "rkom_bid_process_configurations",
            "POWEROPS_",
            RKOMBidProcessConfig,
            RKOMBidList,
        )


class BenchmarkingConfigurationsAPI(AssetAPI):
    def __init__(self, client: CogniteClient, read_dataset: str, write_dataset: str):
        super().__init__(
            client,
            read_dataset,
            write_dataset,
            "benchmarking_configurations",
            "POWEROPS_",
            BenchmarkingConfig,
            BenchmarkingList,
        )


class MarketConfigurationsAPI(AssetAPI):
    def __init__(self, client: CogniteClient, read_dataset: str, write_dataset: str):
        super().__init__(
            client,
            read_dataset,
            write_dataset,
            "market_configurations",
            "market_configuration_",
            MarketConfig,
            MarketConfigsList,
        )


class RKOMBidCombinationConfiguration(AssetAPI):
    def __init__(self, client: CogniteClient, read_dataset: str, write_dataset: str):
        super().__init__(
            client,
            read_dataset,
            write_dataset,
            "rkom_bid_combination_configurations",
            "RKOM_bid_combination_configuration_",
            RKOMBidCombinationConfig,
            RKOMBidCombinationList,
        )
