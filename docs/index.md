# Welcome to PowerOps SDK

## What is it?
The PowerOps SDK is a domain-specific SDK for interacting with Cognite Data Fusion (CDF) for the power operations' domain.

## Main Features

* `cognite.powerops.client.PowerOpsClient` used to interact with CDF in a domain-specific language.
* `cognite.powerops.preprocessor` Preprocessing of SHOP runs processed by CDF.
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
