from typing import ClassVar, List

from cognite.client.data_classes import Asset, Sequence
from pydantic import BaseModel

from cognite.powerops.data_classes.cdf_resource_collection import (
    BootstrapResourceCollection,
    SequenceContent,
    SequenceRows,
)
from cognite.powerops.utils.cdf_utils import simple_relationship
from cognite.powerops.utils.labels import RelationshipLabels


class BidMatrixGeneratorConfig(BaseModel):
    plant_names: List[str]
    default_method: str  # TODO: Enum?
    default_function_external_id: str
    column_external_ids: ClassVar = [
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

    def to_sequence_rows(self) -> SequenceRows:
        return SequenceRows(
            rows=[
                (idx, [plant, self.default_method, self.default_function_external_id])
                for idx, plant in enumerate(self.plant_names)
            ],
            columns_external_ids=self.column_external_ids,
        )

    def to_bootstrap_resources(
        self,
        bid_process_config_asset: Asset,
        price_area: str,
        bid_process_config_name: str,
    ) -> BootstrapResourceCollection:
        bootstrap_resource_collection = BootstrapResourceCollection()

        sequence = self.get_sequence(price_area, bid_process_config_name)
        bootstrap_resource_collection.add(sequence)

        sequence_rows = self.to_sequence_rows()
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
