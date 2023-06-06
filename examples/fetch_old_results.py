import tempfile

from cognite.powerops import PowerOpsClient
from cognite.powerops.logger import configure_debug_logging

configure_debug_logging("DEBUG")

p = PowerOpsClient()

run = p.shop.runs.retrieve(external_id="POWEROPS_SHOP_RUN_ede8c4c0-18b1-41b1-ae40-ea20e037645c")

print(run.is_complete())
print(run.status())

res = run.results()

tmp_dir = tempfile.mkdtemp(prefix="power-ops-sdk-usage")
print(res.logs.post_run.save_to_path(tmp_dir))

# data = res["model.creek_intake.Golebiowski_intake.net_head"]
# res.plot(
#     "model.creek_intake.Golebiowski_intake.net_head",
# )
