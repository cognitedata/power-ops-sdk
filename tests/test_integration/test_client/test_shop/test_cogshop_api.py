from datetime import datetime
from unittest.mock import patch

import requests

from cognite.powerops.client._generated.data_classes import (
    ShopCase,
    ShopCaseWrite,
    ShopFile,
    ShopFileWrite,
    ShopModel,
    ShopModelWrite,
    ShopScenario,
    ShopScenarioWrite,
)
from cognite.powerops.client.powerops_client import PowerOpsClient


def test_create_trigger_shop_case(power_ops_client: PowerOpsClient):
    start_time = datetime(2023, 1, 19, 23)
    end_time = datetime(2023, 1, 29, 23)

    example_case_xid = "example_case_fornebu"
    example_commands_xid = "example_commands"

    shop_file_list = [
        (example_case_xid, example_case_xid, False, ""),
        (example_commands_xid, example_commands_xid, False, "commands"),
    ]

    shop_case = power_ops_client.cogshop.prepare_shop_case(
        shop_file_list=shop_file_list,
        shop_version="15.6.1.0",
        start_time=start_time,
        end_time=end_time,
        model_name="test_model",
        scenario_name="test_scenario",
    )

    # Validate the created shop case object
    assert isinstance(shop_case, ShopCaseWrite)
    assert shop_case.space == "power_ops_instances"
    assert shop_case.external_id.startswith("shopcase:")
    assert shop_case.start_time == start_time
    assert shop_case.end_time == end_time
    assert shop_case.status == "default"

    # Validate the shop file objects connected to the shop case
    shop_files = shop_case.shop_files
    assert len(shop_files) == 2
    assert isinstance(shop_files[0], ShopFileWrite)
    assert isinstance(shop_files[1], ShopFileWrite)
    assert shop_files[0].external_id.startswith("shopfile:")
    assert shop_files[0].space == "power_ops_instances"
    assert shop_files[0].name == example_case_xid
    assert shop_files[0].file_reference == example_case_xid
    assert shop_files[0].file_reference_prefix is None
    assert shop_files[0].order == 1
    assert shop_files[1].external_id.startswith("shopfile:")
    assert shop_files[1].space == "power_ops_instances"
    assert shop_files[1].name == example_commands_xid
    assert shop_files[1].file_reference == example_commands_xid
    assert shop_files[1].file_reference_prefix is None
    assert shop_files[1].order == 2

    # Validate the scenario connected to the shop case
    scenario = shop_case.scenario
    assert isinstance(scenario, ShopScenarioWrite)
    assert scenario.space == "power_ops_instances"
    assert scenario.external_id.startswith("shopscenario:")
    assert scenario.name == "test_scenario"

    # Validate the model connected to the shop case
    model = scenario.model
    assert isinstance(model, ShopModelWrite)
    assert model.external_id.startswith("shopmodel:")
    assert model.name == "test_model"
    assert model.shop_version == "15.6.1.0"

    # Validate the shop case external_id doesn't exist
    res_shop_case = power_ops_client.cogshop.retrieve_shop_case(shop_case.external_id)
    assert res_shop_case is None, "Expected no existing shop case with matching xid before upsert"

    # Upsert the shop case and validate the response exists
    power_ops_client.powermodel.upsert(shop_case)
    res_shop_case = power_ops_client.cogshop.retrieve_shop_case(shop_case.external_id)
    assert res_shop_case is not None, "Expected to find the upserted shop case"
    assert isinstance(res_shop_case, ShopCase)
    assert res_shop_case.status == "default"  # Confirm the status is default after upsert

    # Validate the scenario, model, and files are created and linked correctly from the shop case upsert
    res = power_ops_client.powermodel.shop_based_day_ahead_bid_process.shop_scenario.retrieve(scenario.external_id)
    assert isinstance(res, ShopScenario)
    res = power_ops_client.powermodel.shop_based_day_ahead_bid_process.shop_model.retrieve(model.external_id)
    assert isinstance(res, ShopModel)
    res = power_ops_client.powermodel.shop_based_day_ahead_bid_process.shop_file.retrieve(shop_files[0].external_id)
    assert isinstance(res, ShopFile)
    res = power_ops_client.powermodel.shop_based_day_ahead_bid_process.shop_file.retrieve(shop_files[1].external_id)
    assert isinstance(res, ShopFile)

    # Trigger the shop case and validate the status is updated, mock the API call to simulate the trigger action
    with patch.object(requests, "post") as mock_post:
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"message": "Shop run triggered MOCK"}
        power_ops_client.cogshop.trigger_shop_case(shop_case.external_id)

        expected_url = "https://power-ops-api.staging.bluefield.cognite.ai/power-ops-staging/run-shop-as-service"
        expected_json = {
            "mode": "fdm",
            "runs": [
                {"case_external_id": shop_case.external_id, "write_classic_ts": True, "shop_dump_output_only": False}
            ],
        }
        mock_post.assert_called_once()
        kwargs = mock_post.call_args[1]
        assert kwargs["url"] == expected_url
        assert kwargs["json"] == expected_json

    res_shop_case = power_ops_client.cogshop.retrieve_shop_case(shop_case.external_id)
    assert isinstance(res_shop_case, ShopCase)
    assert res_shop_case.status == "triggered"

    # Clean up: delete the shop case and validate it no longer exists along with its related objects
    power_ops_client.powermodel.delete(external_id=shop_case, space="power_ops_instances")
    res_shop_case = power_ops_client.cogshop.retrieve_shop_case(shop_case.external_id)
    assert res_shop_case is None
    res = power_ops_client.powermodel.shop_based_day_ahead_bid_process.shop_scenario.retrieve(scenario.external_id)
    assert res is None
    res = power_ops_client.powermodel.shop_based_day_ahead_bid_process.shop_model.retrieve(model.external_id)
    assert res is None
    res = power_ops_client.powermodel.shop_based_day_ahead_bid_process.shop_file.retrieve(shop_files[0].external_id)
    assert res is None
    res = power_ops_client.powermodel.shop_based_day_ahead_bid_process.shop_file.retrieve(shop_files[1].external_id)
    assert res is None
