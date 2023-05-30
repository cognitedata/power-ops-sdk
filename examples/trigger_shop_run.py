from cognite.powerops import Case, PowerOpsClient
from cognite.powerops.logger import configure_debug_logging

configure_debug_logging("DEBUG")

p = PowerOpsClient()

case = Case.load_yaml("/path/to/cogshop/test_cogshopsession/test_inputs/run.yaml")
case.add_cut_file("/path/to/cogshop/test_cogshopsession/test_inputs/cut.txt")
case.add_mapping_file(
    "/path/to/cogshop/test_cogshopsession/test_inputs/reservoir_mapping.txt",
    encoding="iso-8859-1",  # default: "utf-8
)
case.add_extra_file("/path/to/cogshop/test_cogshopsession/test_inputs/commands.yaml")

p.shop.runs.trigger(case)
