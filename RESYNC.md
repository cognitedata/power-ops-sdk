# Resync FDM v1

Resource Sync, `resync`, used to sync configuration files with CDF through the CLI tool `powerops`.

[!NOTE]
The current implementation does not support calculating the `connection_losses` so a value must be provided if the
bid generation method requires it.

[!NOTE]
The current implementation requires that "from_type/to_type" is provided in order to find the generator connections to
plants.

[!WARNING]
All generators/plants must be listed in the configuration in order to be processed by resync. A configuration to include
all assets in a model file is in the backlog to be implemented.

[!WARNING]
Current implementation does not clean up any edges that were removed from the configurations, but will only append new
edges to the existing references.

## Configuration

### Configuration files

1. resync configuration
   - Contains all general configuration regarding resync
   - Refer to `power-ops-sdk/tests/data/demo/v1/resync_configuration.yaml` as an example
2. data model configuration [Optional]
   - Contains data type specific configurations
   - One per subfolder in order to have different configurations per subfolder
   - Refer to `power-ops-sdk/tests/data/demo/v1/fornebu/data_model_configuration.yaml` as an example
3. data model population
   - Contains configuration of specific instances
   - Refer to type examples in `power-ops-sdk/tests/data/demo/v1/fornebu` and `power-ops-sdk/tests/data/demo/v1/shared` as an example
   - Currently recommendation is to only use resync for the below types but these can be expanded with further testing
     - market_configuration
     - price_area_information
     - bid_configuration_day_ahead
     - water_value_based_partial_bid_configuration
     - shop_based_partial_bid_configuration
     - generator
     - plant_information
     - shop_attribute_mapping
     - shop_commands
     - shop_model
     - shop_scenario
     - shop_scenario_set

### Default folder structure

```
📦 resync_v1
├─ fornebu
│  ├─ data_model_configuration.yaml
│  ├─ bid_configuration_day_ahead.yaml
│  ├─ water_value_based_partial_bid_configuration.yaml
│  ├─ shop_based_partial_bid_configuration.yaml
│  ├─ generator.yaml
│  └─ ... (all types)
├─ shared
│  ├─ data_model_configuration.yaml
│  ├─ price_area_information.yaml
│  ├─ market_configuration.yaml
│  ├─ shop_commands.yaml
│  └─ ... (all types)
├─ files
│  ├─ model.yaml
│  └─ other_files.yaml
└─ resync_configuration.yaml
```

[!WARNING]
All data model files need to be located in subfolders from the root folder that is specified inside
the `resync_configuration.yaml` in the `working_directory` field.

### Data Model Configuration

The below example configuration will populate every `production_min` property of objects with the type `generator`
by using the data from the `source_file` and navigating to the value in the dictionary that corresponds to the
`extraction_path`.

Any string keys contained inside `[]` will be substituted by that instance's field by that name, ie. the below
example would use the path `model.generator.Holen_G1.p_min` in order to populate the generator with name *Holen_G1*.

Any integer keys contained inside `[]` will fetch the value at that index, ie. like for example this
`extraction_path: "model.plant.[name].main_loss.[0]"` would get the 0 indexed item inside *main_loss*.

The `default_value` will be used for any instances that do not have any values in the designated `extraction_path`

```
generator:
  production_min:
    source_file: "files/model.yaml"
    extraction_path: "model.generator.[name].p_min"
    is_list: False
    default_value: 0.0
    cast_type: float
```

A general configuration for all instances of a specific type can be defined as above in the "data model configuration"
file but to override a configuration on a specific instance you can use the below syntax in the "data model population"
files.

Below is an instance of a plant in the `plant_information.yaml` and the *production_min* property is being
overridden by a custom `extraction_path`. This option currently only supports providing a `source_file` and the
`extraction_path` in the following syntax

`"[SOURCE:<source_file>]<extraction_path>"`

```
- name: Lund
  display_name: Lund
  ordering: 1
  asset_type: plant
  production_min: "[SOURCE:files/model.yaml]model.plant.Lund.p_min"
```

### References

Since there are many fields that references other fields or existing CDF resources you can use their external_id or
name to reference them using the below syntax.

When using `[external_id]` as the prefix like in *market_configuration* below then the following string must be an
exact match to an external_id. This can refer to any type in CDF as long as the full external_id is provided.

When using `[name]` as the prefix like in *price_area* below then the following string must be an exact match to the
name of an instance of the expected type for that field as the expected external_id will be extrapolated from the
inferred type and name. For example, the expected type is price_area so the generated external_id would be
`price_area_no2`

When using `[name|type:DataModelType]` as the prefix like the *partials* below, then it will handle in the same way as
for `[name]` except instead of inferring the type it will use the provided type in order to generate the external_id.
For example, the two *partials* external_ids would become `water_value_based_partial_bid_configuration_plant_lund` and
`shop_based_partial_bid_configuration_plant_lund_set_a`

```
- name: Mixed Configuration
  market_configuration: "[external_id]market_configuration_nord_pool_day_ahead"
  price_area: "[name]NO2"
  bid_date_specification: '[["shift", {"days": -400}], ["floor", "day"]]'
  partials:
    - "[name|type:WaterValueBasedPartialBidConfiguration]Plant Lund"
    - "[name|type:ShopBasedPartialBidConfigurationWrite]Plant Lund Set A"
```

## Usage

Instructions assume Cognite client is configured through `power_ops_config.yaml`

See available commands:

```bash
$ powerops --help
```

Example of showing plan changes provided the configuration file path `resync_v1/resync_configuration.yaml`:

```bash
$ powerops plan_v1 power_ops_config.yaml resync_v1/resync_configuration.yaml
```

Example of showing apply changes provided the configuration file path `resync_v1/resync_configuration.yaml`:

```bash
$ powerops apply_v1 power_ops_config.yaml resync_v1/resync_configuration.yaml
```

[!NOTE]
Current implementation requires suffixing every command with "v1" in order to use the FDM v1 compatible resources.
