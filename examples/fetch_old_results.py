import tempfile
from pprint import pprint

import yaml

from cognite.powerops import PowerOpsClient
from cognite.powerops.logger import configure_debug_logging

configure_debug_logging("DEBUG")

p = PowerOpsClient()

run = p.shop.runs.retrieve(external_id="POWEROPS_SHOP_RUN_ede8c4c0-18b1-41b1-ae40-ea20e037645c")

print(run.in_progress)
print(run.status)

res = run.get_results()

tmp_dir = tempfile.mkdtemp(prefix="power-ops-sdk-usage")
print(res.post_run.save_to_disk(tmp_dir))

print(res.post_run.data["model"]["objective"]["average_objective"]["solver_status"])
pprint(res.post_run.data["model"]["objective"]["average_objective"])
print(yaml.dump(res.post_run.data["model"]["objective"]["average_objective"]))
# res.plot(
#     "model.creek_intake.Golebiowski_intake.net_head",
# )
