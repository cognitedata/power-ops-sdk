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

## [0.25.2] - 07-07-23
### Fixed

* Bug causing the md5 hash used in the `powerops plan` command to be OS dependent. This is now fixed..



## [0.25.1] - 06-07-23
### Changed

* Upgrade to `pydantic` 2.0.


## [0.25.0] - 05-07-23
### Changed

* Refactored package into `bootstrap`, `clients` and `preprocessor` modules. This is likely a breaking change for most users of the package.


## [0.24.1] - 30-06-23
### Removed

* Removed unused settings.


## [0.24.0] - 03-07-23
### Added

* Data Models for bootstrap resources


## [0.23.0] - 29-06-23
### Added

* Re-implemented populating data to DM.


## [0.22.2] - 29-06-23
### Fixed

* Fix for getting PowerOpsClient instance without settings file.

### Changed

* Instantiating PowerOpsClient with config form settings files now has to be done via a separate factory method.


## [0.22.1] - 29-06-23
### Fixed

* Fixed the data model field names in the preprocessor package.


## [0.22.0] - 29-06-23
### Removed

* Dependency on cognite-gql-pygen
* Temporary removed DM code which needs to be rewritten for new DM client.

### Added

* Settings validation.


## [0.21.0] - 28-06-23
### Changed

* Changed how preprocessor fetches shop files from CDF
* Preprocessor will fetch a file from CDF that says explicitly what files to fetch and in what order they should be
loaded to shop prior to a run
* This will support the option for having several cutfiles and module series files per watercourse
* Breaking change for preprocessor as it will fail if the CogShop file config does not exist in CDF

## [0.20.0] - 25-06-23
### Added

* Added file type for shop files of type "module_series"
* Added option to provide additional string in file names and external ID for files uploaded to CDF

## [0.19.1] - 26-06-23
### Fixed

* Adding "generator_" prefix to the external_id of generators

## [0.19.0] - 26-06-23
### Added

* Add p-min to cdf generator object

## [0.18.0] - 23-06-23
### Added

* Added ability to compare the yamls of shop runs and view it in a notebook.
* Example Notebook for showing results and comparing shop runs.


## [0.17.0] - 21-06-23
### Added

* Support for multiple cut files on the preprocessor subpackage.


## [0.16.0] - 21-06-23
### Added

* Add metadata field "no_shop" to bid process config.


## [0.15.2] - 21-06-23
### Added

* Documentation metadata to package, and a minimum introduction to the package in the documentation.


## [0.15.1] - 19-06-23
### Changed

* Upd required versions for cognite-sdk and cognite-gql-pygen.


## [0.15.0] - 19-06-23
### Added

* Ability to find time series keys in the shop generated YAML files, including some filtering.
* Ability to plot several time series from one shop generated YAML file using matplotlib.
* Ability to compare a time series from different shop runs in a plot view if they are accessible by the same keys.


## [0.14.0] - 19-06-23
### Added

* CogShop preprocessor, moved from dedicated repo.

## [0.13.0] - 19-06-23
### Changed

* Removed unnecessary dependencies


## [0.12.0] - 08-06-23
### Added

* Ability to trigger SHOP runs via CogShop.
* Retrieval of ShopRun results.

## [0.11.0] - 08-06-23
### Added

* Allow customers to specify default bid config per price area

## [0.10.0] - 05-06-23
### Added

* Added validation to rkom_bid_combination config for reference to rkom_bid_process external IDs.

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
