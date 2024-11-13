# Welcome to PowerOps SDK

[![release](https://img.shields.io/github/actions/workflow/status/cognitedata/power-ops-sdk/release.yml?style=for-the-badge)](https://github.com/cognitedata/power-ops/actions/workflows/release.yml)
[![Documentation Status](https://readthedocs.com/projects/cognite-power-ops-sdk/badge/?version=latest&style=for-the-badge)](https://cognite-power-ops-sdk.readthedocs-hosted.com/en/latest/?badge=latest)
[![Github](https://shields.io/badge/github-cognite/power_ops_sdk-green?logo=github&style=for-the-badge)](https://github.com/cognitedata/power-ops-sdk)
[![PyPI](https://img.shields.io/pypi/v/cognite-power-ops?style=for-the-badge)](https://pypi.org/project/cognite-power-ops/)
[![Downloads](https://img.shields.io/pypi/dm/cognite-power-ops?style=for-the-badge)](https://pypistats.org/packages/cognite-power-ops)
[![GitHub](https://img.shields.io/github/license/cognitedata/power-ops-sdk?style=for-the-badge)](https://github.com/cognitedata/power-ops-sdk/blob/master/LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg?style=for-the-badge)](https://github.com/ambv/black)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json&style=for-the-badge)](https://github.com/astral-sh/ruff)
[![mypy](https://img.shields.io/badge/mypy-checked-000000.svg?style=for-the-badge&color=blue)](http://mypy-lang.org)

## What is it?

The PowerOps SDK is a domain-specific SDK for interacting with Cognite Data Fusion (CDF) for the power operations' domain.

## Main Features

* `cognite.powerops.client.PowerOpsClient` used to interact with CDF in a domain-specific language.
* Resource Sync, `resync`, used to sync configuration files with CDF through the CLI tool `powerops`.

## Installation

```bash
pip install cognite-power-ops
```

## Configuration

Configuration of the `PowerOpsClient` and `resync` is done through a yaml file and environment variables.

### YAML configuration

The configuration is in `.yaml` format and the path to the configuration file must be explicitly provided. Refer to the [example config file](power_ops_config.yaml) for the most up to date example of required fields.

Secrets should not be written directly in this configuration file as the file is intended to be committed to git. Instead use the following syntax to refer to an environment variable as a value.

```yaml
project: "${PROJECT}"
base_url: "https://${CLUSTER}.cognitedata.com"
```

If you are using a `.env` file etc. then you must handle loading the proper environment variables prior to instantiating `PowerOpsClient` or running `resync`.

## Usage

### Run Resync

Refer to the [resync documentation](RESYNC.md).

### PowerOpsClient

Using YAML configuration:

```python
from dotenv import load_dotenv
from cognite.powerops.client import PowerOpsClient

load_dotenv()
power_ops_client = PowerOpsClient.from_config("power_ops_config.yaml")

```

Using an existing CogniteClient:

```python
from cognite.powerops.client import PowerOpsClient

# Refer to the Cognite SDK documentation for the different ways to instantiate a CogniteClient
cognite_config = {} # dict with configuration
cognite_client = CogniteClient.load(cognite_config)

# Instantiate PowerOpsClient with existing CogniteClient
power_ops_client = PowerOpsClient(client=cognite_client, read_dataset="xid_dataset", write_dataset="xid_dataset")

```

For more examples on using the PowerOpsClient, see the examples section of the documentation.
