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

Configuration of the `PowerOpsClient` and `resync` is done through settings files.


### Settings Files
The settings file are in `.toml` format. By default, the SDK will look for two settings files:
  1. `settings.toml` in the current directory.
  2. `.secrets.toml` in the current directory.

The motivation for splitting them is to avoid checking in secrets into Git.

Example of settings files:

`settings.toml`:
```toml
[cognite]
  login_flow = "interactive"
  project = "<cdf-project>"
  tenant_id = "<tenant-id>"
  cdf_cluster = "<cdf-cluster>"
  client_id = "<client-id>"

[powerops]
  read_dataset = "uc:000:powerops"
  write_dataset = "uc:000:powerops"
  monitor_dataset = "uc:po:monitoring"
```

`.secrets.toml`
```toml
[cognite]
  client_secret = "<client-secret>"
```

**Note:** You can configure which settings files to use by setting the environment variable `SETTINGS__FILES` to a semicolon-separated list of file names.

```python
import os

os.environ["SETTINGS_FILES"] = ".my_settings.toml;.secrets.my_secrets.toml"
```

## Usage

### Run Resync

See available commands:

```bash
$ powerops --help
```

Example of showing planned changes:

```bash
$ powerops plan tests/data/demo Dayahead
```

### PowerOpsClient

```python
from cognite.powerops.client import PowerOpsClient

client = PowerOpsClient.from_settings()

client.shop.runs.trigger()
```

For more examples, see the examples section of the documentation.
