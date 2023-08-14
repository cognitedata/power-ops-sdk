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
* The CLI tool `powerops` used to populate CDF from configuration files.

## Installation

```bash
pip install  cognite-power-ops
```

## Configuration

### Settings Files

Settings files are optional, but some features of the SDK rely on them being present.

By default, `settings.toml` is configured for `power-ops-staging` project on CDF. To change this:
  1. Copy `settings.toml` to `.secrets.toml` (create it).
  2. Edit `.secrets.toml` as needed.

Values from the two files are merged together, with those from `.secrets.toml` taking precedence.

`.secrets.toml` is ignored by Git, but `settings.toml` is not.


### Environment Variables

Settings values can be set via environment variables, e.g:

```
SETTINGS__COGNITE__CLIENT_SECRET=abcdefghijklm...
```

Names of the variables follow the structure from [settings.toml](settings.toml).

Environment vars take precedence over settings files.


### Current Directory

SDK will look for the settings files in the current directory, so make sure to run code from the repository root.


## Usage

### Run Bootstrap

See available commands:

```bash
$ powerops --help
```

Example of showing planed changes:

```bash
$ powerops plan tests/test_bootstrap/data/demo Dayahead
```


### Other

See [examples](examples).
