from pprint import pprint

from cognite.powerops import Case, PowerOpsClient
from cognite.powerops.resync._logger import configure_debug_logging

configure_debug_logging("DEBUG")

p = PowerOpsClient.from_settings()

case = Case.from_yaml_file("/path/to/cogshop/test_cogshopsession/test_inputs/run.yaml")
case.add_cut_file("/path/to/cogshop/test_cogshopsession/test_inputs/cut.txt")
case.add_mapping_file(
    "/path/to/cogshop/test_cogshopsession/test_inputs/reservoir_mapping.txt",
    encoding="iso-8859-1",  # default: "utf-8
)
case.add_extra_file("/path/to/cogshop/test_cogshopsession/test_inputs/commands.yaml")

run = p.shop.runs.trigger(case)
# run = p.shop.runs._upload_to_cdf(case)  # upload but don't trigger

print(run.in_progress)
run.wait_until_complete()
print(run.in_progress)


results = run.get_results()
print(results)
pprint(results.post_run.data["commands"])
