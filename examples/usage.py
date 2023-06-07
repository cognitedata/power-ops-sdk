from cognite.powerops.client.powerops_client import PowerOpsClient

powerops = PowerOpsClient()

fornebu = powerops.watercourses.retrieve("Fornebu")

fornebu_42 = fornebu.copy2(name="Fornebu_test_42")
# fornebu_42 = powerops.watercourses.copy(fornebu, name="Fornebu_test_42")

print(fornebu_42.model_raw)
# edit model_raw.yaml
fornebu_42 = powerops.watercourses.update(fornebu_42)


config = powerops.configurations.retrieve("default?")

config_42 = powerops.configurations.copy("testing_42")  # not needed [if] don't keep config in CDF
config_42.market.max_price = 1234
config_42 = powerops.configurations.update(config_42)  # TODO is configuration fully local or needs .update() ?

results = powerops.shop.run(external_id="???", configuration=config, shop_version=None)

# TODO inspect results

models = powerops.shop.models.list()  # list[ShopModelTemplate]
sample_model = models[0]

sample_model = powerops.shop.models.retrieve(external_id="123")

sample_model.clone().with_overrides({})
sample_model.clone(overrides={})

sample_case = powerops.shop.cases.create(model_template=sample_model, commands=[])

sample_run = powerops.shop.runs.trigger(case=sample_case)

print(sample_run.in_progress)


sample_run_results = powerops.shop.runs.trigger(case=sample_case).get_results()


print(sample_run_results.cplex)
print(sample_run_results.shop)
