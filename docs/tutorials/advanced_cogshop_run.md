# Advanced Cogshop Run

This tutorial summarizes the workflow in `tutorials/advanced_cogshop_run.ipynb`.

It demonstrates two common patterns:

1. Running a case with multiple input files.
2. Reusing existing `ShopScenario` instances across multiple `ShopCase` runs.

## Preparation

Typical setup:

- Initialize `PowerOpsClient` from config.
- Verify credentials and project access.
- Prepare local SHOP input files.

Example helper for uploading files:

```python
from pathlib import Path

def upload_file(local_file_name: str, external_id: str) -> str:
    file_path = (Path("example_case_files") / local_file_name).resolve()
    uploaded = powerops.cdf.files.upload(
        path=str(file_path),
        external_id=external_id,
        name=local_file_name,
        overwrite=True,
    )
    return uploaded.external_id
```

## Example 1: Multiple SHOP Files in One Case

### Step 1: Upload input files

```python
case_file = upload_file("b_example_fornebu_without_commands.yaml", "example_case_fornebu")
commands_file = upload_file("b_example_commands.yaml", "example_commands")
```

### Step 2: Prepare `ShopCase`

```python
shop_file_list = [
    (case_file, "b_example_fornebu_without_commands.yaml", True, ""),
    (commands_file, "b_example_commands.yaml", True, ""),
]

shop_case = powerops.cogshop.prepare_shop_case(
    shop_file_list=shop_file_list,
    shop_version="16.0.3",
    start_time=start_time,
    end_time=end_time,
    model_name="Demo model",
    scenario_name="Demo scenario",
)
```

### Step 3: Write and retrieve case

```python
powerops.powermodel.upsert(shop_case)
retrieved_shop_case = powerops.cogshop.retrieve_shop_case(shop_case.external_id)
```

### Step 4: Trigger execution

```python
powerops.cogshop.trigger_shop_case(shop_case.external_id)
```

### Step 5: Inspect results

```python
result_list = powerops.cogshop.list_shop_results_for_case(shop_case.external_id, limit=3)
for result in result_list:
    print(result.external_id)
```

## Example 2: Reuse Existing Scenarios

When running multiple cases with the same model/scenario setup, create scenario objects once and reuse them.

### Step 1: Create reusable scenarios

```python
from cognite.powerops.client._generated.data_classes import ShopModelWrite, ShopScenarioWrite

reusable_scenario = ShopScenarioWrite(
    name="Reusable scenario 16.0.3",
    model=ShopModelWrite(name="Reusable model", shop_version="16.0.3"),
)
```

### Step 2: Upsert reusable scenario

```python
powerops.powermodel.upsert(reusable_scenario)
```

### Step 3: Create cases referencing the existing scenario

```python
shop_case_reuse = powerops.cogshop.prepare_shop_case_with_existing_scenario(
    shop_file_list=shop_file_list,
    start_time=start_time,
    end_time=end_time,
    shop_scenario_reference=reusable_scenario.external_id,
)
```

### Step 4: Trigger and inspect

```python
powerops.powermodel.upsert(shop_case_reuse)
powerops.cogshop.trigger_shop_case(shop_case_reuse.external_id)
results = powerops.cogshop.list_shop_results_for_case(shop_case_reuse.external_id, limit=3)
```

## Cleanup

After testing, remove created objects so repeated tutorial runs stay deterministic.

### Delete created `ShopCase` / `ShopScenario` / `ShopModel` nodes

```python
# Example IDs from this tutorial
cleanup_case_ids = [
    shop_case.external_id,
    shop_case_reuse.external_id,
]

cleanup_scenario_ids = [
    reusable_scenario.external_id,
]

for external_id in cleanup_case_ids + cleanup_scenario_ids:
    powerops.powermodel.delete(external_id)
```

### Delete uploaded CDF files used by tutorial runs

```python
# File external IDs returned from upload_file(...)
file_external_ids = [case_file, commands_file]

for file_id in file_external_ids:
    powerops.cdf.files.delete(external_id=file_id)
```

### Verify cleanup

```python
# Should return empty/None after deletion
print(powerops.cogshop.retrieve_shop_case(shop_case.external_id))
print(powerops.cdf.files.retrieve(external_id=case_file))
```

## Source Notebook

The complete runnable notebook is stored in the repository at:

- [`tutorials/advanced_cogshop_run.ipynb`](https://github.com/cognitedata/power-ops-sdk/blob/main/tutorials/advanced_cogshop_run.ipynb)
