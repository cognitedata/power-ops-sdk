from cognite.powerops import Case, PowerOpsClient

p = PowerOpsClient(read_dataset="uc:001:sandbox", write_dataset="uc:001:sandbox")

case = Case.load_yaml("/Users/fran/Downloads/case1.yaml")

p.shop.runs.trigger("Test_case_1", case)
