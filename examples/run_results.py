import logging

from cognite.powerops.client.powerops_client import PowerOpsClient
from cognite.powerops.client.shop_api import ShopRun, ShopRunResult

# root_logger = logging.getLogger()
# root_logger.setLevel(logging.INFO)

logger = logging.getLogger(__name__)


powerops = PowerOpsClient()
# # TODO inspect results

SAMPLE_SHOP_RUN_EVENT = "POWEROPS_SHOP_RUN_6336e7ae-722a-4c3a-a9bb-d719922e727f"

# sample_cdf_shop_run_event = powerops.cdf.events.retrieve(
#     external_id=SAMPLE_SHOP_RUN_EVENT)

# sample_shop_event = ShopRunEvent.from_event(sample_cdf_shop_run_event)
# sample_shop_run = ShopRun(po_client=powerops, shop_run_event=sample_shop_event)
sample_shop_run: ShopRun = powerops.shop.runs.retrieve(SAMPLE_SHOP_RUN_EVENT)

print(f"sample_shop_run: {sample_shop_run}")

sample_run_results: ShopRunResult = sample_shop_run.wait_until_complete()

print("-------")
print(sample_run_results.logs.cplex.read())
print("-------")
print(sample_run_results.logs.shop.file_metadata.external_id)
print("-------")
print(sample_run_results.logs.post_run.read())

# print(sample_run_results.logs.cplex())

# sample_run_results = powerops.shop.runs.trigger(
#     case=sample_case).wait_until_complete()


# sample_run_results.logs.cplex.print()
# sample_run_results.logs.shop.print()
