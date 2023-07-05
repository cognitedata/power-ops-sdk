from cognite.powerops.clients.cogshop import CogShopClient

from .api.shop_api import ShopModelsAPI
from .api.shop_result_files_api import ShopFilesAPI
from .api.shop_results_api import ShopRunResultsAPI
from .api.shop_run_api import ShopRunsAPI


class ShopClient:
    def __init__(self, cogshop: CogShopClient):
        self._cogshop = cogshop
        self.models = ShopModelsAPI(cogshop)
        self.runs = ShopRunsAPI(cogshop)
        self.results = ShopRunResultsAPI(cogshop)
        self.files = ShopFilesAPI(cogshop)
