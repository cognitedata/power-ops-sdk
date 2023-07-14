from cognite.client import CogniteClient

from cognite.powerops.clients._api_client import CogShopAPIs
from cognite.powerops.clients.data_set_api import DataSetsAPI

from .api.shop_api import ShopModelsAPI
from .api.shop_result_files_api import ShopFilesAPI
from .api.shop_results_api import ShopRunResultsAPI
from .api.shop_run_api import ShopRunsAPI


class ShopClient:
    def __init__(self, client: CogniteClient, cogshop: CogShopAPIs, data_set_api: DataSetsAPI, cogshop_version: str):
        self.models = ShopModelsAPI(cogshop)
        self.files = ShopFilesAPI(client)
        self.results = ShopRunResultsAPI(client, self.files)
        self.runs = ShopRunsAPI(client, data_set_api, self.results, cogshop_version)
