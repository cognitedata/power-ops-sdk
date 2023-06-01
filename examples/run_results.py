import logging

from cognite.powerops.client.powerops_client import PowerOpsClient

logger = logging.getLogger(__name__)


powerops = PowerOpsClient()
# # TODO inspect results

SAMPLE_SHOP_RUN_EVENT = "POWEROPS_SHOP_RUN_6336e7ae-722a-4c3a-a9bb-d719922e727f"

sample_shop_run = powerops.shop.runs.retrieve(SAMPLE_SHOP_RUN_EVENT)

print(f"sample_shop_run: {sample_shop_run}")

sample_shop_run.wait_until_complete()
sample_run_results = sample_shop_run.results()

print(f"sample_run_results: {sample_run_results.success}")
print("-------")

_path = sample_run_results.logs.shop.save_to_path()

print(f"_path: {_path}")

# print("-------")
# print(sample_run_results.logs.shop.file_metadata.external_id)
print("-------")
print(sample_run_results.logs.post_run.data["commands"])

# print(sample_run_results.logs.cplex())

# sample_run_results = powerops.shop.runs.trigger(
#     case=sample_case).wait_until_complete()


# sample_run_results.logs.cplex.print()
# sample_run_results.logs.shop.print()
