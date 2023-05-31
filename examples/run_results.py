from cognite.powerops.client.powerops_client import PowerOpsClient

powerops = PowerOpsClient()

# glomma = powerops.watercourses.retrieve("Glomma")
# config = powerops.configurations.retrieve("default?")

# results = powerops.shop.run(external_id="???", configuration=config, shop_version=None)
# print(glomma)
# # edit model_raw.yaml
# # glomma_copy = powerops.watercourses.update(glomma_copy)


# config = powerops.configurations.retrieve("default?")

# config_42 = powerops.configurations.copy("testing_42")  # not needed [if] don't keep config in CDF

# results = powerops.shop.run(external_id="???", configuration=config, shop_version=None)

# print("RESULTS", results)
# # TODO inspect results


models = powerops.shop.models.list()  # list[ShopModelTemplate]
sample_model = models[0]

sample_model = powerops.shop.models.retrieve(model_id="123")

sample_model.clone().with_overrides({})
sample_model.clone(overrides={})

sample_case = powerops.shop.cases.create(model_template=sample_model, commands=[])

sample_run = powerops.shop.runs.trigger(case=sample_case)

print(sample_run.is_complete())


sample_run_results = powerops.shop.runs.trigger(case=sample_case).wait_until_complete()


sample_run_results.logs.cplex.print()
sample_run_results.logs.shop.print()
