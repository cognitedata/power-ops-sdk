from dataclasses import dataclass
from typing import List, Optional

from cognite.client.data_classes import Sequence
from pandas import DataFrame

from cognite.powerops.data_classes.cdf_resource_collection import BootstrapResourceCollection, SequenceContent


@dataclass
class ShopOutputDefinition:
    shop_object_type: str
    shop_attribute_name: str
    cdf_attribute_name: str
    unit: str
    is_step: bool = True


@dataclass
class ShopOutputConfig:
    watercourse: str
    shop_output_definitions: Optional[List[ShopOutputDefinition]] = None

    @property
    def default_output_definitions(self) -> List[ShopOutputDefinition]:
        return [
            ShopOutputDefinition("market", "sale_price", "price", "EUR/MWh"),
            ShopOutputDefinition("market", "sale", "sales", "MWh"),
            ShopOutputDefinition("plant", "production", "production", "MW"),
            ShopOutputDefinition("plant", "consumption", "consumption", "MW"),
            ShopOutputDefinition("reservoir", "water_value_global_result", "water_value", "EUR/Mm3"),
            ShopOutputDefinition("reservoir", "energy_conversion_factor", "energy_conversion_factor", "MWh/Mm3"),
        ]

    def to_dataframe(self):
        if not self.shop_output_definitions:
            self.shop_output_definitions = self.default_output_definitions
        df = DataFrame(self.shop_output_definitions)
        df["is_step"] = df["is_step"].map({True: "True", False: "False"})
        return df

    @classmethod
    def to_sequence(cls, watercourse: str) -> Sequence:
        sequence_external_id = f"SHOP_{watercourse.replace(' ', '_')}_output_definition"
        column_def = [
            {"valueType": "STRING", "externalId": "shop_object_type"},
            {"valueType": "STRING", "externalId": "shop_attribute_name"},
            {"valueType": "STRING", "externalId": "cdf_attribute_name"},
            {"valueType": "STRING", "externalId": "unit"},
            {"valueType": "STRING", "externalId": "is_step"},
        ]

        return Sequence(
            name=sequence_external_id.replace("_", " "),
            description="Defining which SHOP results to output to CDF (as time series)",
            external_id=sequence_external_id,
            columns=column_def,
            metadata={
                "shop:watercourse": watercourse,
                "shop:type": "output_definition",
            },
        )

    def to_bootstrap_resources(self) -> BootstrapResourceCollection:
        bootstrap_resources = BootstrapResourceCollection()
        sequence = ShopOutputConfig.to_sequence(watercourse=self.watercourse)

        bootstrap_resources.add(sequence)
        bootstrap_resources.add(SequenceContent(sequence_external_id=sequence.external_id, data=self.to_dataframe()))
        return bootstrap_resources
