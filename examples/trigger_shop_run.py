from cognite.powerops import Case, PowerOpsClient
from cognite.powerops.logger import configure_debug_logging

configure_debug_logging("DEBUG")

p = PowerOpsClient(read_dataset="uc:001:sandbox", write_dataset="uc:001:sandbox")

case = Case.load_yaml("/Users/fran/Downloads/case1.yaml")

p.shop.runs.trigger("Test_case_1", case)
