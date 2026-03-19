# Installation

```bash
pip install cognite-power-ops
```

## Configuration

Configuration of `PowerOpsClient` and `resync` is done through YAML and environment variables.

Use the example configuration as a starting point:

- [power_ops_config.yaml](https://github.com/cognitedata/power-ops-sdk/blob/main/power_ops_config.yaml)

### Environment Variable Substitution

Keep secrets out of committed YAML files and reference environment variables instead:

```yaml
project: "${PROJECT}"
base_url: "https://${CLUSTER}.cognitedata.com"
```

If you use a `.env` file, load it before creating `PowerOpsClient` or running `resync`.
