from cognite.powerops import PowerOpsClient
from cognite.powerops.logger import configure_debug_logging

configure_debug_logging("DEBUG")

p = PowerOpsClient()

run = p.shop.runs.retrieve(external_id="POWEROPS_SHOP_RUN_dd32b39b-556a-4680-8649-3263bdf85544")

print(run.is_complete())

#
# path = run.download_from_cdf()
# res = Results.load_yaml(path)
#
# res = run.get_results()
#
# data = res["model.creek_intake.Golebiowski_intake.net_head"]
# res.plot(
#     "model.creek_intake.Golebiowski_intake.net_head",
# )
#
