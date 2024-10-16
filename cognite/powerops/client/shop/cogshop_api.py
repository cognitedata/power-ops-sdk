from urllib.parse import urlparse

import requests
from cognite.client import CogniteClient


class CogShopAPI:
    def __init__(self, client: CogniteClient):
        self._cdf = client

    def _shop_url_cshaas(self) -> str:
        project = self._cdf.config.project

        cluster = urlparse(self._cdf.config.base_url).netloc.split(".", 1)[0]

        environment = ".staging" if project == "power-ops-staging" else ""

        return f"https://power-ops-api{environment}.{cluster}.cognite.ai/{project}/run-shop-as-service"

    def trigger_shop_case(self, shop_case_external_id: str):
        def auth(r: requests.PreparedRequest) -> requests.PreparedRequest:
            auth_header_name, auth_header_value = self._cdf._config.credentials.authorization_header()
            r.headers[auth_header_name] = auth_header_value
            return r

        shop_url = self._shop_url_cshaas()
        shop_body = {
            "mode": "fdm",
            "runs": [{"case_external_id": shop_case_external_id}],
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
