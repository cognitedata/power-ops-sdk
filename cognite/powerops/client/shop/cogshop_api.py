import datetime
from urllib.parse import urlparse

import requests
from cognite.client import CogniteClient

from cognite.powerops.client._generated.v1._api_client import PowerOpsModelsV1Client
from cognite.powerops.client._generated.v1.data_classes import (
    ShopCaseWrite,
    ShopFileWrite,
    ShopModelWrite,
    ShopResultList,
    ShopScenarioWrite,
)
from cognite.powerops.client._generated.v1.data_classes._core import (
    ResourcesWriteResult,
)


class CogShopAPI:
    def __init__(self, cdf: CogniteClient, po: PowerOpsModelsV1Client):
        self._cdf = cdf
        self._po = po

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

    def prepare_shop_case(
        self,
        files: list[tuple[str, str, bool, str]],
        shop_version: str,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        model_name: str,
        scenario_name: str,
        model_external_id: str | None = None,
        scenario_external_id: str | None = None,
        case_external_id: str | None = None,
    ) -> ShopCaseWrite:
        """
        Prepare a SHOP case that can be written to cdf.
        External ids must be unique. If they are not provided, they will be generated.
        Args:
            files: List of 4-tuples and every item is expected.
                    It is assumed that the order of the list if the order the files should be loaded into SHOP.
                Format:
                `file_reference`: external if of file in CDF.
                `file_name`: Name of the file.
                `is_ascii`: Whether the file is in ASCII format.
                `labels`: Labels to be added to the fil, use "" if no labels

            start_time: Start of time range SHOP is optimized over
            end_time: End of time range SHOP is optimized over
            shop_version: Version of SHOP to use (e.g. '16.0.2'or '15.7.0.0'), required.

            scenario_name: Name of the scenario. Required, does not have to be unique
            model_name: Name of the model. Required, does not have to be unique
            scenario_external_id: External ID of the scenario. Optional, must be unique
            model_external_id: External ID of the model. Optional, must be unique
            case_external_id: External ID of the SHOP case. Optional, must be unique
        Returns:
            ShopCaseWrite: A SHOP case that can be written to CDF
        """
        model_write = ShopModelWrite(name=model_name, external_id=model_external_id, shop_version=shop_version)

        scenario_write = ShopScenarioWrite(
            name=scenario_name,
            external_id=scenario_external_id,
            model=model_write,
        )

        shop_files_write = [
            ShopFileWrite(
                name=file_name,
                fileReference=file_reference,
                isAscii=is_ascii,
                labels=labels,
                order=i + 1,  # Order is 1-indexed
            )
            for i, (file_reference, file_name, is_ascii, labels) in enumerate(files)
        ]

        case_write = ShopCaseWrite(
            external_id=case_external_id,
            start_time=start_time,
            end_time=end_time,
            scenario=scenario_write,
            shop_files=shop_files_write,
        )
        return case_write

    def write_shop_case(self, shop_case: ShopCaseWrite) -> ResourcesWriteResult:
        """
        Write a SHOP case to CDF.
        Args:
            shop_case: SHOP case to write to CDF
        Returns:
            ResourcesWriteResult: Result of the write operation
        """
        return self._po.upsert(shop_case)

    def list_shop_results_for_case(self, case_external_id: str, limit: int = 3) -> ShopResultList:
        """
        View the result of a SHOP case.
        Args:
            case_external_id: External ID of the SHOP case
            limit: Number of results to return, -1 for all results
        """
        result_list: ShopResultList = self._po.shop_based_day_ahead_bid_process.shop_result.list(
            case_external_id=case_external_id, limit=limit
        )
        return result_list

    def list_shop_versions(self) -> list[str]:
        """List the available version of SHOP remotely  in CDF.
        Does not include versions in local version of CShaaS.
        SHOP releases should have the following format:
        'SHOP-{VERSION}-pyshop-python{py_version}.linux.zip'
        """
        # todo? Add an endpoint to list the available versions of SHOP via powerops API?
        return [file.name for file in self._cdf.files.list(metadata={"shop:type": "shop-release"}, limit=-1)]
