from urllib.parse import urlparse

import requests
from cognite.client import CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    ShopCase,
    ShopCaseWrite,
)

SHOP_VERSION_FALLBACK = "15.5.0.0"


class CogShopAPI:
    def __init__(
        self,
        client: CogniteClient,
        dataset_id: int,
    ):
        self._cdf = client
        self._dataset_id = dataset_id
        self._CONCURRENT_CALLS = 5

    def _shop_url_shaas(self) -> str:
        project = self._cdf.config.project

        cluster = urlparse(self._cdf.config.base_url).netloc.split(".", 1)[0]

        if project == "power-ops-staging":
            environment = ".staging"
        elif project in {
            "lyse-dev",
            "lyse-prod",
            "heco-dev",
            "heco-prod",
            "oe-dev",
            "oe-prod",
        }:
            environment = ""
        else:
            raise ValueError(f"SHOP As A Service has not been configured for project name: {project!r}")

        return f"https://power-ops-api{environment}.{cluster}.cognite.ai/{project}/run-shop-as-service"

    def trigger_shop_case(self, shop_run: ShopCaseWrite | ShopCase):
        def auth(r: requests.PreparedRequest) -> requests.PreparedRequest:
            auth_header_name, auth_header_value = self._cdf._config.credentials.authorization_header()
            r.headers[auth_header_name] = auth_header_value
            return r

        shop_url = self._shop_url_shaas()
        shop_body = {
            "mode": "fdm",
            "runs": [{"case_external_id": shop_run.external_id}],
        }

        response = requests.post(
            url=shop_url,
            json=shop_body,
            auth=auth,
        )
        response.raise_for_status()

    def list_shop_versions(self) -> list[str]:
        """List the available version of SHOP remotely  in CDF.
        Does not include versions in local version of CShaaS.
        SHOP releases should have the following format:
        'SHOP-${{VERSION}}-pyshop-python{py_version}.linux.zip'
        """
        # todo? Add an endpoint to list the available versions of SHOP via powerops API?
        return [file.name for file in self._cdf.files.list(metadata={"shop:type": "shop-release"}, limit=-1)]
