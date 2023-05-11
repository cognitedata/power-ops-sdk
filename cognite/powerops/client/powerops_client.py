from cognite.powerops.client.dm.client import get_power_ops_dm_client
from cognite.powerops.config import WatercourseConfig


class TimeseriesTransformation:
    def visualize(self):
        ...


class WatercourseAPI:
    def retrieve(self, external_id: str) -> WatercourseConfig:
        ...

    def update(self, watercourse: WatercourseConfig):
        ...


class TimeseriesTransformationsAPI:
    def retrieve(self, externa_id: str) -> TimeseriesTransformation:
        ...


class ConfigurationAPI:
    def retrieve(self, external_id: str):
        ...


class SHOPAPI:
    def __init__(self):
        self.timeseries_transformations = TimeseriesTransformationsAPI()
        self.configuration = ConfigurationAPI()

    def run(self, external_id: str, configuration: dict, shop_version: str) -> dict:
        ...


class PowerOpsClient:
    def __init__(self):
        self.watercourses = WatercourseAPI()
        self.shop = SHOPAPI()

        # low-level clients:
        self.dm = get_power_ops_dm_client()  # manage DM items (instances) directly
        self.cdf = self.dm._client  # CogniteClient plus DM v3 client (Nodes, Edges, Spaces, etc.)
