from cognite.client import CogniteClient

from cognite.powerops.client._generated._api_client import CogShopAPIs
from cognite.powerops.client.data_set_api import DataSetsAPI
from cognite.powerops.client.shop.api.shop_run_api import ShopRunsAPI

from .api.shop_result_files_api import ShopFilesAPI
from .api.shop_results_api import ShopRunResultsAPI


class SHOPRunAPI:
    """
    The ShopClient is a client for the Shop API. It is built on top of the Cognite Python SDK

    Examples:

    Initialization of SHOP client:

        >>> from cognite.powerops.client import PowerOpsClient
        >>> client = PowerOpsClient()
        >>> shop_client = client.shop

    """

    def __init__(self, client: CogniteClient, cogshop: CogShopAPIs, data_set_api: DataSetsAPI, cogshop_version: str):
        self.files = ShopFilesAPI(client)
        self.results = ShopRunResultsAPI(client, self.files)
        self.runs = ShopRunsAPI(client, data_set_api, self.results, cogshop_version)
