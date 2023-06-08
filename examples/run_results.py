import logging

from cognite.powerops.client.powerops_client import PowerOpsClient

logger = logging.getLogger(__name__)


powerops = PowerOpsClient()


SAMPLE_SHOP_RUN_EVENT = "POWEROPS_SHOP_RUN_ede8c4c0-18b1-41b1-ae40-ea20e037645c"
# SAMPLE_SHOP_RUN_EVENT = "POWEROPS_SHOP_RUN_6336e7ae-722a-4c3a-a9bb-d719922e727f"
# SAMPLE_SHOP_RUN_EVENT = "POWEROPS_SHOP_RUN_5fd00d38-5782-4bff-8fea-fa4d8fe57f0a"

# SAMPLE_SHOP_RUN_EVENT = "POWEROPS_SHOP_RUN_da57af5b-d8fe-4986-bfcb-a51b8bdf00b7_cloned_1684923299.8"

sample_shop_run = powerops.shop.runs.retrieve(SAMPLE_SHOP_RUN_EVENT)

# print(f"sample_shop_run: {sample_shop_run}")

sample_shop_run.wait_until_complete()
sample_run_results = sample_shop_run.get_results()

print(f"sample_run_results: {sample_run_results}")
print("-------")

post_run = sample_run_results.post_run
# post_run.plot("model.generator.BRAS(3210)_G1.eff_head")
keys = post_run.list_model_time_series_keys(
    matches_object_type="plant",
    # matches_object_name="BRAS(3210)_G1",
    # matches_object_name="KVER(3237)",
    matches_attribute_name="production",
)

print("potential keys:", keys)
# post_run.plot(dot_key=keys[2])

# post_run.plot(dot_keys=(keys[4], keys[5]))
post_run.plot(dot_keys=keys)


# _path = sample_run_results.post_run.save()
# print(f"_path: {_path}")


# # OBJECTIVE
# objective = sample_run_results.objective_function
# print(f"objective_function: {objective}")

# print(objective.data)
# print(objective.data_as_str())
# print(objective.watercourse)
# print(objective.penalty_breakdown)
# print(objective.penalty_breakdown_as_str())
