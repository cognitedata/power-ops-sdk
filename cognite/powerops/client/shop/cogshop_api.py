import datetime
from urllib.parse import urlparse

import requests
from cognite.client import CogniteClient

from cognite.powerops.client._generated.v1._api_client import PowerOpsModelsV1Client
from cognite.powerops.client._generated.v1.data_classes import (
    ShopCase,
    ShopCaseWrite,
    ShopFileWrite,
    ShopModelWrite,
    ShopResult,
    ShopResultList,
    ShopScenarioWrite,
)
from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE


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

    def _validate_shop_scenario_reference(
        self, shop_scenario_reference: str | ShopScenarioWrite
    ) -> str | ShopScenarioWrite:
        """Checks if the provided shop scenario is valid.
        Either it can be a Write object which is usable when uploaded or an external ID of an already existing scenario.
        """
        if isinstance(shop_scenario_reference, ShopScenarioWrite):
            # allow the write object to be used directly
            return shop_scenario_reference

        elif isinstance(shop_scenario_reference, str):
            # if the scenario reference is a string, it is assumed to be an external id
            if scenario := self._po.shop_based_day_ahead_bid_process.shop_scenario.retrieve(
                external_id=shop_scenario_reference
            ):
                return scenario.external_id

        raise ValueError(f"Invalid shop scenario reference: {shop_scenario_reference}.")

    def prepare_shop_case_with_existing_scenario(
        self,
        shop_file_list: list[tuple[str, str, bool, str]],
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        shop_scenario_reference: str | ShopScenarioWrite,
        case_external_id: str | None = None,
    ) -> ShopCaseWrite:
        """
        Prepare a SHOP case that can be written to CDF, specifying just a scenario we wish to use.

        case_external_id must be unique if provided. If it is not provided, it will be auto-generated.


        Args:
          shop_file_list: List of (file_reference, file_name, is_ascii, label) tuples,
                          in the order the files should be loaded.
             `file_reference`: external id of file in CDF.
             `file_name`: Name of the file.
             `is_ascii`: Whether the file is in ASCII format.
             `label`: Label to be added to the file, use "" if no labels
          start_time: Start of time range SHOP is optimized over. Required.
          end_time: End of time range SHOP is optimized overs. Required.
          shop_scenario_reference: An external id of an existing scenario or a `ShopScenarioWrite` instance. Required.
          case_external_id: External ID of the ShopCase instance that will be created. Optional, must be unique.

        Returns:
            ShopCaseWrite: A SHOP case that can be written to CDF
        """

        shop_files_write = [
            ShopFileWrite(
                name=file_name,
                fileReference=file_reference,
                isAscii=is_ascii,
                label=label,
                order=i + 1,  # Order is 1-indexed
            )
            for i, (file_reference, file_name, is_ascii, label) in enumerate(shop_file_list)
        ]

        case_write = ShopCaseWrite(
            start_time=start_time,
            end_time=end_time,
            scenario=self._validate_shop_scenario_reference(shop_scenario_reference),
            shop_files=shop_files_write,
            **({"external_id": case_external_id} if case_external_id else {}),
        )
        return case_write

    def prepare_shop_case(
        self,
        shop_file_list: list[tuple[str, str, bool, str]],
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
        NB! This is not the recommended way to create a SHOP case.

        Prepare a SHOP case that can be written to cdf.
        External ids must be unique. If they are not provided, they will be generated.

        In this case, `ShopScenario` as and its `ShopModel` are mostly superfluous.
        However, they are still added as nearly empty objects in order to set the SHOP version.

        Args:
          shop_file_list: List of (file_reference, file_name, is_ascii, label) tuples,
                          in the order the files should be loaded.
            `file_reference`: external id of file in CDF.
            `file_name`: Name of the file.
            `is_ascii`: Whether the file is in ASCII format.
            `label`: Label to be added to the file, use "" if no labels

          start_time: Start of time range SHOP is optimized over
          end_time: End of time range SHOP is optimized over
          shop_version: Version of SHOP to use (e.g. '16.0.2'or '15.7.0.0'). Required.

          scenario_name: Name of the scenario. Required, does not have to be unique
          model_name: Name of the model. Required, does not have to be unique
          scenario_external_id: External ID of the ShopScenario instance that will be created. Optional, must be unique
          model_external_id: External ID of the ShopModel instance that will be created. Optional, must be unique
          case_external_id: External ID of the ShopCase instance that will be created. Optional, must be unique

        Returns:
            ShopCaseWrite: A SHOP case that can be written to CDF
        """
        model_write = ShopModelWrite(
            name=model_name,
            shop_version=shop_version,
            **({"external_id": model_external_id} if model_external_id else {}),
        )

        scenario_write = ShopScenarioWrite(
            name=scenario_name,
            model=model_write,
            **({"external_id": scenario_external_id} if scenario_external_id else {}),
        )

        return self.prepare_shop_case_with_existing_scenario(
            shop_file_list=shop_file_list,
            start_time=start_time,
            end_time=end_time,
            shop_scenario_reference=scenario_write,
            case_external_id=case_external_id,
        )

    def retrieve_shop_case(self, case_external_id: str) -> ShopCase:
        """Retrieve a shop case from CDF"""
        return self._po.shop_based_day_ahead_bid_process.shop_case.retrieve(external_id=case_external_id)

    def list_shop_results_for_case(self, case_external_id: str, limit: int = 3) -> ShopResultList:
        """
        View the result of a SHOP case.
        Args:
            case_external_id: External ID of the SHOP case
            limit: Number of results to return, -1 for all results
        """
        result_list: ShopResultList = self._po.shop_based_day_ahead_bid_process.shop_result.list(
            case=case_external_id, limit=limit
        )
        return result_list

    def retrieve_shop_result(self, result_external_id: str) -> ShopResult:
        """Retrieve a shop result from CDF"""
        return self._po.shop_based_day_ahead_bid_process.shop_result.retrieve(external_id=result_external_id)

    def list_shop_versions(self) -> list[str]:
        """List the available version of SHOP in CDF. Does not include versions that are "local" to CShaaS.
        SHOP releases should have the following format:
        'SHOP-{VERSION}-pyshop-python{py_version}.linux.zip' and the metadata {'shop:type': 'shop-release'}.
        """
        # todo? Add an endpoint to list the available versions of SHOP via powerops API?
        return [file.name for file in self._cdf.files.list(metadata={"shop:type": "shop-release"}, limit=-1)]

    def retrieve_shop_case_graphql(self, case_external_id: str) -> ShopCase:
        """Retrieve a shop case from CDF using GraphQL.
        Instead of the external of IDs of ShopScenario, ShopModel and ShopFiles, the actual instances are fetched.
        This is provided as a fallback option for cases when `retrieve_shop_case` does not perform well."""
        graphql_response = self._po.shop_based_day_ahead_bid_process.graphql_query(_shop_case_query(case_external_id))
        if graphql_response:
            shop_case: ShopCase = graphql_response[0].as_read()
        else:
            raise ValueError(f"Failed to fetch ShopCase instance with external_id: {case_external_id}.")
        return shop_case

    def list_shop_results_graphql(self, case_external_id: str, limit: int) -> list[ShopResult]:
        """Retrieve a shop result, based on a case external id using GraphQL.
        This is provided as a fallback option for cases when `list_shop_results_for_case` does not perform well"""

        graphql_response = self._po.shop_based_day_ahead_bid_process.graphql_query(
            _shop_result_query(case_external_id, limit=limit)
        )

        if graphql_response is not None:
            shop_results: list[ShopResult] = [item.as_read() for item in graphql_response]
        else:
            raise ValueError(f"Failed to fetch ShopResult referencing ShopCase with external_id: {case_external_id}.")
        return shop_results


# Helper function for rendering graphql queries


def _shop_case_query(external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> str:
    """Render a GraphQL query to fetch a ShopCase instance by external_id."""
    query_template = """
    query RetrieveShopCaseByExternalId {{
      getShopCaseById(instance: {{ space: "{space}", externalId: "{external_id}" }}) {{
        items {{
          __typename
          createdTime
          endTime
          externalId
          lastUpdatedTime
          space
          startTime
          scenario {{
            createdTime
            externalId
            lastUpdatedTime
            name
            source
            space
            outputDefinition {{
              items {{
                attributeName
                createdTime
                externalId
                isStep
                lastUpdatedTime
                name
                objectName
                objectType
                space
                unit
              }}
            }}
            model {{
              createdTime
              externalId
              lastUpdatedTime
              modelVersion
              name
              shopVersion
              space
            }}
          }}
          shopFiles (sort: {{order: ASC}}) {{
            items {{
              createdTime
              externalId
              fileReferencePrefix
              isAscii
              label
              lastUpdatedTime
              name
              order
              space
              fileReference {{
                externalId
              }}
            }}
          }}
        }}
      }}
    }}
    """
    return query_template.format(space=space, external_id=external_id)


def _shop_result_query(case_external_id: str, limit: int) -> str:
    """Render a GraphQL query to fetch a ShopResult instance by case external id."""
    query_template = """
    query RetrieveShopResultByCaseExternalId {{
        listShopResult(
        filter: {{case: {{externalId: {{eq: "{case_external_id}"}} }} }}
        first: {limit}
    ) {{
        items {{
        __typename
        space
        createdTime
        externalId
        lastUpdatedTime
        preRun {{
            externalId
            id
            uploadedTime
            name
            metadata
            labels
        }}
        postRun {{
            externalId
            id
            labels
            uploadedTime
            name
            metadata
        }}
        messages {{
            externalId
            id
            labels
            uploadedTime
            name
            metadata
        }}
        cplexLogs {{
            externalId
            id
            labels
            uploadedTime
            name
            metadata
        }}
        case {{
            externalId
            lastUpdatedTime
            createdTime
        }}
        alerts {{
            items {{
            externalId
            lastUpdatedTime
            description
            createdTime
            alertType
            title
            time
            statusCode
            }}
        }}

        }}
    }}
    }}
    """
    return query_template.format(case_external_id=case_external_id, limit=limit)
