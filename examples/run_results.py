import logging

from cognite.powerops.client.powerops_client import PowerOpsClient

logger = logging.getLogger(__name__)


powerops = PowerOpsClient()

# POWEROPS_DAY_AHEAD_BID_MATRIX_CALCULATION_06877a8c-8acf-4adf-9618-00ec98de84bd


SAMPLE_SHOP_RUN_EVENT_1 = "POWEROPS_SHOP_RUN_47d45ac8-391a-4a60-8153-335784ffbc48"

sample_run_results_1 = powerops.shop.runs.retrieve(SAMPLE_SHOP_RUN_EVENT_1).get_results()

print(f"{sample_run_results_1=}")

# # OBJECTIVE
# objective = sample_run_results_1.objective_function
# print(f"objective_function: {objective}")

# print(objective.data)
# print(objective.data_as_str())
# print(objective.watercourse)
# print(objective.penalty_breakdown)
# print(objective.penalty_breakdown_as_str())


# PLOTTING A POST RUN TIME SERIES

post_run_1 = sample_run_results_1.post_run

# found_keys = post_run_1.find_time_series(
#     matches_object_types=["generator", "plant"],
#     matches_object_names=["HARP(2232)_G2", "HINO(390171)_G1", "NVIN(2225)", "HUND(2238)"],
#     matches_attribute_names=["production"],
# )
# print(found_keys)
# post_run_1.plot(found_keys)


# # # COMPARING TWO SHOP RUN RESULTS
# Second run from same AHEAD_BID_MATRIX_CALCULATION event
SAMPLE_SHOP_RUN_EVENT_2 = "POWEROPS_SHOP_RUN_71010201-3cd9-4df5-8193-aca3e5d323af"


COMPARISON_KEY = "model.market.1.buy_price"
sample_run_results_2 = powerops.shop.runs.retrieve(SAMPLE_SHOP_RUN_EVENT_2).get_results()


post_run_2 = sample_run_results_2.post_run

runs = (
    post_run_1,
    post_run_2,
)

# powerops.shop.results.compare.plot_time_series(
#     post_run_list=runs,
#     comparison_key=COMPARISON_KEY,
#     # labels=["Example 1", "Example 2"],  # optional labels
# )

deep_diff_md = powerops.shop.results.compare.yaml_difference_md(*runs, ("Run 1", "Run 2"))
print("------")
print(deep_diff_md)
