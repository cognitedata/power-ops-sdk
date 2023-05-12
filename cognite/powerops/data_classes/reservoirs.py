from typing import Optional

from cognite.powerops.data_classes.shared import AssetModel


class Reservoir(AssetModel):
    display_name: Optional[str]
    ordering: Optional[int]
