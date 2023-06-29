import re

from typing import Optional
from cognite.powerops.config import WatercourseConfig

from cognite.powerops.data_classes.shared import AssetModel

from cognite.client.data_classes import Asset

from cognite.powerops.utils.common import replace_nordic_letters


class Reservoir(AssetModel):
    display_name: Optional[str]
    ordering: Optional[int]

    @classmethod
    def from_shop_case_reservoirs(
        cls, 
        shop_case_reservoirs_dict: dict,
        watercourse_config: WatercourseConfig,
        # There is the option to return a ReservoirList
        # instead, unsure how that looks for writing later.
        ) -> list["Reservoir"]:
        reservoirs = []

        for res_name in shop_case_reservoirs_dict:
            res_name = replace_nordic_letters(res_name)
            # I think there is a way to look up this in the config
            # and get the display name and ordering key from there
            # maybe like how it is done in the generator.py
            reservoirs.append(
                cls(name=res_name,
                    display_name=watercourse_config.reservoir_display_name(res_name),
                    ordering=watercourse_config.reservoir_ordering_key(res_name),
                    external_id=f"reservoir_{res_name}",
                    )
            )
        return reservoirs

    def asset(self):
        return Asset(
            external_id=self.external_id,
            name=self.name,
            parent_external_id="reservoirs",
            metadata={
                "display_name": self.display_name or re.sub(r"\([0-9]+\)", "", self.name),
                "ordering": str(self.ordering) if self.ordering else "999"
            },
        )
