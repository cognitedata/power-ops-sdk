
from cdf_auth import get_client_demo # just a function to get a CogniteClient
from cognite import pygen # pip install cognite-pygen
from cognite.powerops import PowerOpsClient

import datetime
import random

if __name__ == "__main__":
    client = get_client_demo()
    po_client = PowerOpsClient(read_dataset="powerops:misc", write_dataset="powerops:misc", cogshop_version="0.0.1", config=client.config)

    space = "final_thomi"
    data_model_external_id = "compute_BenchmarkingDayAhead"
    data_model_version = "1"
    data_model_id = (space, data_model_external_id, data_model_version)
    #bench_client = pygen.generate_sdk_notebook(data_model_id, client)

    import cognite.powerops.client._generated.v1.data_classes as data_classes

    #po_client.v1.day_ahead_configuration.bid_configuration_day_ahead.list()
    bid_config_ext_ids = ["bid_configuration_day_ahead_multi_scenario_2_no2", "bid_configuration_day_ahead_water_value_no2_full", "bid_configuration_day_ahead_no2_combination", "bid_configuration_day_ahead_price_independent_no2"]
    bid_configs = po_client.v1.day_ahead_configuration.bid_configuration_day_ahead.retrieve(external_id=bid_config_ext_ids)
    # shop_models = po_client.v1.shop_based_day_ahead_bid_process.shop_model.list(space="sp_power_ops_instance", limit=1000)
    from cognite.powerops.client._generated.v1.data_classes import BidConfigurationDayAheadWrite

    shop_model_dummy_1 = data_classes.ShopModelWrite(
        externalId="shop_model_benchmarking_dummy_1",
        name="Fornebu B",
        shopVersion="15.4.1",
        model="SHOP_Fornebu_model"
    )
    shop_model_dummy_2 = data_classes.ShopModelWrite(
        externalId="shop_model_benchmarking_dummy_2",
        name="Lysakerelva B",
        shopVersion="15.4.1",
        model="SHOP_Fornebu_model"
    )
    shop_scenario_dummy_1 = data_classes.ShopScenarioWrite(
        externalId="shop_scenario_benchmarking_dummy_1",
        name="Fornebu B Benchmarking",
        model=shop_model_dummy_1
    )
    shop_scenario_dummy_2 = data_classes.ShopScenarioWrite(
        externalId="shop_scenario_benchmarking_dummy_2",
        name="Lysakerelva B Benchmarking",
        model=shop_model_dummy_2,
    )
    shop_case_dummy_1 = data_classes.ShopCaseWrite(
        externalId="shop_case_benchmarking_dummy_1",
        name="Lysakerelva B Benchmarking dummy",
        scenario=shop_scenario_dummy_1,
    )
    shop_case_dummy_2 = data_classes.ShopCaseWrite(
        externalId="shop_case_benchmarking_dummy_2",
        name="Lysakerelva B Benchmarking dummy",
        scenario=shop_scenario_dummy_2,
    )
    shop_result_dummy_1 = data_classes.ShopResultWrite(
        externalId="shop_result_benchmarking_dummy_1",
        name="Fornebu B Benchmarking dummy",
        case=shop_case_dummy_1
    )
    shop_result_dummy_2 = data_classes.ShopResultWrite(
        externalId="shop_result_benchmarking_dummy_2",
        name="Lysakerelva B Benchmarking dummy",
        case=shop_case_dummy_2,
    )

    shop_results_upserted = po_client.v1.upsert([shop_result_dummy_1, shop_result_dummy_2])

    bid_sources = [
        BidConfigurationDayAheadWrite(space="sp_power_ops_model_benchmarking", externalId=bid_config.external_id, name=bid_config.name, bidDateSpecification="[]")
        for bid_config in bid_configs] + []

    bid_sources = [bid_config.external_id for bid_config in bid_configs] + ["upper_bound"]

    #bid_documents_upserted = bench_client.upsert(bid_sources)

    from pprint import pprint
    #pprint(shop_results_upserted)


    def get_value(bid_source_external_id: str, delivery_date: str, model_external_id: str) -> float:
        if bid_source_external_id == "upper_bound":
            return 0
        return random.random() * -1

    results = []
    delivery_dates = [datetime.date(2024, 5, day) for day in range(1, 32)]
    dummy_shop_results = [shop_result_dummy_1, shop_result_dummy_2]

    for bid_source in bid_sources:
        for delivery_date in delivery_dates:
            for dummy_shop_result in dummy_shop_results:

                results.append(
                    data_classes.BenchmarkingResultDayAheadWrite(
                        externalId=f"benchmarking_result_day_ahead_{dummy_shop_result.case.scenario.model.external_id}_{bid_source}_{delivery_date}_1",
                        bidSource=bid_source,
                        deliveryDate=delivery_date,
                        bidGenerated=None,
                        shopResult=dummy_shop_result.external_id,
                        isSelected=True,
                        value=get_value(bid_source, delivery_date, dummy_shop_result.case.scenario.model.external_id)
                ))
    po_client.v1.upsert(results)

    bm_config = data_classes.BenchmarkingConfigurationDayAheadWrite(
        externalId="day_ahead_benchmarking_configuration_no2",
        name="Day Ahead Benchmarking Configuration Dummy",
        bidConfigurations=bid_config_ext_ids,
        shopStartSpecification="[]",
        shopEndSpecification="[]",
        assetsPerShopModel=[data_classes.ShopModelWithAssetsWrite(
            externalId=f"shop_model_to_assets_dummy_{shop_model.external_id[-1:]}",
            shopModel=shop_model.external_id,
            powerAssets=[],
            productionObligations=[],
        ) for shop_model in [shop_model_dummy_1, shop_model_dummy_2]],

    )
    upserted_bm_config = po_client.v1.upsert(bm_config)

    po_client.v1.benchmarking_day_ahead.benchmarking_result_day_ahead.list()

    po_client.v1.benchmarking_day_ahead.shop_result.list()

    po_client.v1.benchmarking_day_ahead.benchmarking_configuration_day_ahead.list()
