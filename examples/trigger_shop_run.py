from cognite.powerops import Case, PowerOpsClient
from cognite.powerops.logger import configure_debug_logging

configure_debug_logging("DEBUG")

p = PowerOpsClient()

case = Case.load_yaml(
    "/Volumes/Projects/cognite/powerops/cog-shop/tests/cogshop/test_cogshopsession/test_inputs/run.yaml"
)
case.add_cut_file("/Volumes/Projects/cognite/powerops/cog-shop/tests/cogshop/test_cogshopsession/test_inputs/cut.txt")
case.add_mapping_file(
    "/Volumes/Projects/cognite/powerops/cog-shop/tests/cogshop/test_cogshopsession/test_inputs/reservoir_mapping.txt",
    encoding="iso-8859-1",  # default: "utf-8
)
case.add_extra_file(
    "/Volumes/Projects/cognite/powerops/cog-shop/tests/cogshop/test_cogshopsession/test_inputs/commands.yaml"
)

# run = p.shop.runs.trigger(case)
run = p.shop.runs._upload_to_cdf(case)  # upload but don't trigger

print(run.is_complete)
run.wait_until_complete()
print(run.is_complete)


results = run.get_results()
print(results.success)
print(results.logs.post_run.data["commands"])
