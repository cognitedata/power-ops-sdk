Cognite PowerOps SDK
==========================
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

[Documentation](https://cognite-power-ops-sdk.readthedocs-hosted.com/en/latest/)

## Installation

To install this package:
```bash
$ pip install cognite-power-ops
```


## Configuration

Settings files are optional, but some features of the SDK rely on them being present.

By default, `settings.toml` is configured for `power-ops-staging` project on CDF. To change this:
  1. Copy `settings.toml` to `.secrets.toml` (create it).
  2. Edit `.secrets.toml` as needed.

Values from the two files are merged together, with those from `.secrets.toml` taking precedence.

`.secrets.toml` is ignored by Git, but `settings.toml` is not.


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
