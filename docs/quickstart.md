# Quickstart

Using YAML configuration:

```python
from dotenv import load_dotenv
from cognite.powerops.client import PowerOpsClient

load_dotenv()
power_ops_client = PowerOpsClient.from_config("power_ops_config.yaml")
```

Using an existing `CogniteClient`:

```python
from cognite.client import CogniteClient
from cognite.powerops.client import PowerOpsClient

# Refer to Cognite SDK docs for supported initialization patterns.
cognite_config = {}  # dict with Cognite client configuration
cognite_client = CogniteClient.load(cognite_config)

# Instantiate PowerOpsClient with existing CogniteClient
power_ops_client = PowerOpsClient(client=cognite_client)
```

You can access the underlying Cognite client from PowerOps client when needed:

```python
cdf_client = power_ops_client.cdf
```
