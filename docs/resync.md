# Resync

Resource Sync (`resync`) synchronizes PowerOps configuration files with CDF through the `powerops` CLI.

`resync` generates toolkit files that can be used for detailed diffs before deployment.  
Since toolkit does not automatically identify stale objects for deletion, `resync purge` can delete nodes and edges not defined in generated toolkit files.

## Configuration Files

`resync` uses three main configuration layers:

1. **Resync configuration**  
   General settings (for example, working directory, model versions, spaces).
2. **Data model configuration (optional)**  
   Type-level extraction rules shared by groups of instances.
3. **Data model population files**  
   Instance-level values and references.

Commonly used object types include:

- `market_configuration`
- `price_area_information`
- `bid_configuration_day_ahead`
- `water_value_based_partial_bid_configuration`
- `shop_based_partial_bid_configuration`
- `generator`
- `plant_information`
- `shop_attribute_mapping`
- `shop_commands`
- `shop_model`
- `shop_scenario`
- `shop_scenario_set`

## References in Population Files

References support multiple patterns:

- `[external_id]...` for exact external ID references.
- `[name]...` to resolve references from expected type + name.
- `[name|type:DataModelType]...` when type must be explicit.

This is especially useful when linking configurations across models (for example market config, scenarios, and partial bid configurations).

## Usage

```bash
powerops --help
powerops pre-build power_ops_config.yaml resync/configuration.yaml
powerops purge power_ops_config.yaml resync/configuration.yaml --dry-run
powerops purge power_ops_config.yaml resync/configuration.yaml
```

Recommended flow:

1. Generate toolkit files (`pre-build`)
2. Review diff via toolkit
3. Run purge in dry-run mode
4. Execute purge when dry-run output is validated

## Known Limitations

- Some bid-generation paths require explicit `connection_losses` values.
- Some extraction logic requires `from_type` and `to_type` to resolve generator/plant connections.
- Generators and plants must currently be listed explicitly in configuration.
