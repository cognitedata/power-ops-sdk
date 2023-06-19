import logging

from cognite.powerops.client.powerops_client import PowerOpsClient

logger = logging.getLogger(__name__)


powerops = PowerOpsClient()


SAMPLE_SHOP_RUN_EVENT_1 = "POWEROPS_SHOP_RUN_ede8c4c0-18b1-41b1-ae40-ea20e037645c"

sample_run_results_1 = powerops.shop.runs.retrieve(SAMPLE_SHOP_RUN_EVENT_1).get_results()

print(f"{sample_run_results_1=}")

# OBJECTIVE
objective = sample_run_results_1.objective_function
print(f"objective_function: {objective}")

# print(objective.data)
print(objective.data_as_str())
print(objective.watercourse)
# print(objective.penalty_breakdown)
print(objective.penalty_breakdown_as_str())


# PLOTTING A POST RUN TIME SERIES

post_run_1 = sample_run_results_1.post_run

found_keys = post_run_1.find_time_series(
    matches_object_types=["generator", "plant"],
    matches_object_names=["KVER(3237)", "REND(3192)", "BRAS(3210)_G1"],
    matches_attribute_names="production",
)
print(found_keys)
# post_run_1.plot(found_keys)


# COMPARING TWO SHOP RUN RESULTS
# This is an arbitrary second run, so the difference is not meaningful
SAMPLE_SHOP_RUN_EVENT_2 = "POWEROPS_SHOP_RUN_6336e7ae-722a-4c3a-a9bb-d719922e727f"

COMPARISON_KEY = "model.market.1.buy_price"

sample_run_results_2 = powerops.shop.runs.retrieve(SAMPLE_SHOP_RUN_EVENT_2).get_results()

print(f"{sample_run_results_2=}")

post_run_2 = sample_run_results_2.post_run
runs = (
    post_run_1,
    post_run_2,
)

powerops.shop.results.compare.plot_time_series(
    post_run_list=runs,
    comparison_key=COMPARISON_KEY,
    labels=["Example 1", "Example 2"],  # optional labels
)
