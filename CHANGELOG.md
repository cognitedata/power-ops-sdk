# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Changes are grouped as follows
- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon-to-be removed features.
- `Improved` for transparent changes, e.g. better performance.
- `Removed` for now removed features.
- `Fixed` for any bug fixes.
- `Security` in case of vulnerabilities.

## [0.9.2] - 30-05-23
### Fixed

* Fixed bug for overriding CDF parameters from env variables

## [0.9.1] - 26-05-23
### Fixed

* Typo in warning message for plant display name
* Removed redundant warning message for reservoir display name and order

## [0.9.0] - 25-05-23
### Changed

* Changed location of all config related class definitions located in dataclasses module
to config.py module
* Deleted unused modules

## [0.8.0] - 23-05-23
### Added

* Basic handling for YAML case files.
* pytest now runs doctests.

### Improved

* Shorter imports for commonly used files

### Changed

* Renamed `powerops.core` attribute to `powerops.cdf`

## [0.7.1] - 23-05-23
### Fixed

* Fixed bug with external id being stored as int in bootstrap resources

## [0.7.0] - 22-05-23
### Added

* Added test for the water value based bid generation (WVBBG) time series contextualization
* Changed the mapping format for wvbbg from csv to yaml.
* Added time series contextualization for generators as well

## [0.6.0] - 22-05-23
### Added

* Added support for reading configuration parameters for Cognite Client from env variables in addition to yaml file.

## [0.5.0] - 16-05-23
### Added

* Support adding direct head values to water value based bid generation (WVBBG) time series contextualization.

## [0.4.2] - 15-05-23
### Fixed

* Shop files external ID suffix to get correct preview print with bootstrap "plan"


## [0.4.1] - 15-05-23
### Fixed

* Import errors in `PowerOpsClient`


## [0.4.0] - 12-05-23
### Added

* Scaffolding for a `SHOPApi` implementation.


## [0.3.1] - 12-05-23
### Fixed

* Fixed missing `README.md` in package metadata.


## [0.3.0] - 12-05-23
### Added

* Added a `PowerOpsClient` with support for `list` and `retrieve` of `asset` resources

## [0.2.2]
### Fixed

Fixed name for RKOM bid configuration assets and sequences with correct auction name.

## [0.2.1]
### Added

Added some mapping functionality for creating time series mappings in utils module.


## [0.2.0]
### Added

Added bootstrap module with CLI support for running bootstrap with "plan" and "apply".

## [0.1.0]

Initial commit.
