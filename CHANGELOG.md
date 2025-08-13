# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Changes are grouped as follows:

- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon-to-be removed features.
- `Improved` for transparent changes, e.g. better performance.
- `Removed` for now removed features.
- `Fixed` for any bug fixes.
- `Security` in case of vulnerabilities.

## [1.0.0] - 2025-08-14
### Removed
* Removed all classes and functions marked as deprecated
  * SHOPRunAPI
  * _logger
  * cdf_labels
  * utils.helpers
  * utils.identifiers
  * utils.lookup
  * utils.navigation
  * utils.cdf.resource_creation
  * utils.cdf.calls (only the event/relationship based functions)

### Changed
* Restructured utils module to eliminate nested utils
  * `utils.retry.api` is moved to `utils.retry`
  * `utils.cdf.extraction_pipelines` is moved to `utils.extraction_pipelines`
  * `utils.cdf.calls` to `utils.retrieve`
* Moved `get_data_set_from_config` from `utils.cdf.datasets_calls` into `utils.retrieve`
* Moved `cognite.powerops.client._generated.v1.` to `cognite.powerops.client._generated.`
* Renamed `PowerOpsModelsV1Client` to `PowerOpsModelsClient`
* Renamed `PowerOpsClient.v1` to `PowerOpsClient.powermodel`

## [0.114.2] - 2025-08-13
### Added
* Added `get_data_set_from_config` utility function to retrieve the correct CDF dataset for each type (READ, WRITE, MONITOR, PROCESS) from a DataSetConfiguration.

## [0.114.1] - 2025-07-31
### Changed
* Adding process data set to DataSetConfiguration Container and View

## [0.114.0] - 2025-07-31
### Changed
* Adding DataSetConfiguration Container and View

## [0.113.3] - 2025-07-29
### Deprecated
* Marked all old functionality related to Asset/Event/Relationship solution as deprecated

## [0.113.2] - 2025-07-28
### Fixed
* Regenerate pygen portion of SDK to get bug fixes

## [0.113.1] - 2025-07-11
### Changed
* Switch from using `Optional` to `| None` type annotations

## [0.113.0] - 2025-07-08
### Fixed
* Regenerate pygen portion of SDK to avoid unexpected retrieve behavior for classes using inheritance

## [0.112.1] - 2025-06-19
### Added
* Set ShopCase status in `prepare_shop_case` to default
* Set ShopCase status in `trigger_shop_case` to triggered
* Add `retrieve_connections` arg to `retrieve_shop_case`

## [0.112.0] - 2025-06-11
### Added
* Added `status` as enum to ShopCase

## [0.111.2] - 2025-05-06
### Added
* Optional flag to omit writing classic time series in `trigger_shop_case`

## [0.111.1] - 2025-02-13
### Improved
* PR-write back script

## [0.111.0] - 2025-01-30
### Removed
* Removed `list_shop_results_graphql` function from `cogshop` module

### Improved
* Ensured that tutorial notebooks are still valid


## [0.110.1] - 2025-01-20
### Changed
* Rerun pygen version 0.99.60

## [0.110.0] - 2025-01-20
### Added
* Data model `product_CogShop`-data model now also as the view `ShopPenaltyReport`.


## [0.109.0] - 2024-12-31
### Fixed
* Resync build handles direct relations that are none

## [0.108.0] - 2024-12-20
### Added
* Resync purge to clean up any nodes & edges not included in the toolkit files generated from resync pre_build

## [0.107.0] - 2024-12-11
### Added
* Resync pre_build to create toolkit files from resync configurations
### Deprecated
* Resync `plan` and `apply`

## [0.106.0] - 2024-12-09
### Added
* `name` property for ShopTimeResolution

## [0.105.0] - 2024-12-09
### Changed
* Resync source file reference is now a full path reference
### Removed
* Resync file uploading

## [0.104.4] - 2024-12-06
### Fixed
* Switch curve properties to be float64 instead of float32

## [0.104.3] - 2024-12-03
### Added
* Possibility to specify which CogShop service (`prod` or `staging`) to use

## [0.104.2] - 2024-11-27
### Improved
* Better error handling in `Transformation.load`

## [0.104.1] - 2024-11-27
### Added
* `ShopTimeResolution` to `ShopScenario`

## [0.104.0] - 2024-11-25
### Removed
* Removed v0 pygen generated classes
* Removed DayAhead Trigger API

## [0.103.1] - 2024-11-22
### Added
* production_max to generator

## [0.103.0] - 2024-11-21
### Changed
* Simplify usage of cogshop api module and update examples

### Removed
* Removed outdated usage guides.

## [0.102.3] - 2024-11-12
### Removed
* Support for automatically using `load_dotenv`, end user is responsible for loading in required environment variables
### Fixed
* Make `pygen` a default dependency again

## [0.102.2] - 2024-11-05
### Removed
* Plotting functionality for `SHOPResultFile` class, and `matplotlib` as a dependency
### Changed
* Made `pygen` a `dev` dependency

## [0.102.1] - 2024-11-05
### Changed
* Specifying `rich` as a separate dependency instead of as an extra for `typer`

## [0.102.0] - 2024-10-28
### Removed
* Custom parsing of env variables to follow `SETTINGS__COGNITE__<PROPERTY>` pattern
* PowerOpsClient.from_client() removed, use PowerOpsClient() directly instead
* PowerOpsClient.from_settings() & .from_toml() removed, use PowerOpsClient.from_config() instead
* Removed cognite.powerops.utils.cdf._cdf_auth as no longer needed in updated auth configuration
* Removed cognite.powerops.utils.cdf._settings as no longer needed in updated auth configuration
* Removed all helper functions in cognite.powerops.utils.serialization except load_yaml

### Changed
* Input arguments to Resync plan/apply needs path to client config file
* Input to `PowerOpsClient` takes an initialized *CogniteClient* instead of a *ClientConfig*
* Configuration file format changed to YAML and utilizes CogniteClient.load() to allow for more flexible client configuration

## [0.101.1] - 2024-10-21
### Changed
* Rename example cases to Fornebu and Stavanger cases


## [0.101.0] - 2024-10-16
### Removed
* Resync for asset & v0
* Support for python 3.9


## [0.100.0] - 2024-10-14
### Added
* Support for triggering Shop using a ShopCase instance
* Tutorial for usage of the generated SDK in relation to Shop

### Removed
* `shop_as_a_service` boolean to PowerOps Client instantiation
* `cogshop_version` image tag to PowerOps Client instantiation

### Deprecated
* Marked asset based shop trigger, as well as other modules, for deprecation.
## [0.99.0] - 2024-10-11
### Changed
* Removed `PowerAsset` from `product_CogShop`-data model and added `ShopResult`, `ShopTimeSeries`, and `Alert`


## [0.98.4] - 2024-09-09
### Fix
* Unpinned SDK dependency against toolkit and only used pinned version with dev dependencies

## [0.98.3] - 2024-08-26
### Added
* Added SHOPcase to public modules of data_classes

### Changed
* Fixed bug in lazy loading shop.data_classes.SHOPCase

## [0.98.2] - 2024-08-22
### Added
* Added optional shop_run_external_id to shop.shop_run_api.trigger_single_casefile

### Changed
* Switched to lazy loading of shop.data_classes.SHOPCase case to a dictionary, to improve performance where data is not being accessed or changed.

## [0.98.1] - 2024-08-12
### Added
* Verify that external IDs are blow the 255 character limit

## [0.98.0] - 2024-08-09
### Changed
* Restructure of PowerOps `shop_run_api` to remove duplication of logic
* Update of example notebooks in `/docs/tutorials/` to reflect current structure of the SDK

## [0.97.2] - 2024-07-25
### Added
* Add py.typed to support type hints when using library and mypy compatibility

## [0.97.1] - 2024-07-18
### Fixed
* Configure the v1 data model external id factory to not override external_id's as provided since it's default behavior
  was overriding external_id's from object retrieved from CDF and converted to write objects for updates given the
  .as_write() method

## [0.97.0] - 2024-07-15
### Added
* added linked_time_series to bid_matrix_information view and regenerated the powerops SDK

## [0.96.2] - 2024-07-08
### Added
* set the default external id generator function for `v1` data models, does not affect existing resource creation
  as the manually provided external id will take precedence

## [0.96.1] - 2024-07-02
### Fixed
* import from `cognite-toolkit` to new path
* replace deprecated `np.Nan` with `np.nan`

## [0.96.0] - 2024-06-24
### Added
* Support for benchmarking data model and it's types

## [0.95.2] - 2024-06-04
### Fixed
* Fix from setting configuration of triggering SHOP As A Service through SHOP client.

## [0.95.1] - 2024-06-04
### Added
* Support for triggering SHOP As A Service through SHOP client.

## [0.95.0] - 2024-06-04
### Added
* Type DateSpecification to replace previous string format for fields bidDateSpecification, startSpecification, and endSpecification
  * With default value for property processingTimezone as UTC
  * With default value for property resultingTimezone as UTC
* Type ShopOutputTimeSeriesDefinition to replace previous static sequence `SHOP_output_definition`
* Property outputDefinition in ShopScenario as a list of ShopOutputTimeSeriesDefinition
* Default value for properties in ShopAttributeMapping
  * With default value for property retrieve as "RANGE"
  * With default value for property aggregation as "MEAN"
* Added fileReferencePrefix property to ShopFile to be used if no file external id is provided

### Changed
* Renamed ShopPartialBidMatrixCalculationInput to MultiScenarioPartialBidMatrixCalculationInput
* Replaced "date specification" properties as a string JSON type with a new type DateSpecification
* Type of property cogShopFilesConfig in ShopModel to be a list of ShopFile
* Type of property penstockHeadLossFactors to a list of float instead of JSON
* Renamed objectiveSequence property in ShopResult to objectiveValue
* Type of property objectiveValue (previously objectiveSequence) in ShopResult to a JSON instead of sequence
* Renamed property intermediateBidMatrices in BidMatrixInformation to underlyingBidMatrices
* Renamed property input in all FunctionOutput views to functionInput
* Renamed properties on Generator
  * startCost to startStopCost
  * startStopCost to startStopCostTimeSeries
* Renamed property in ShopModel to avoid python name conflicts
  * version to modelVersion
* Changed space names
  * sp_power_ops_instance to power_ops_instances
  * sp_power_ops_models to power_ops_core
  * sp_power_ops_types to power_ops_types

### Removed
* Removed property extraFiles from ShopModel
* Removed unused/redundant y property from EfficiencyCurve container

### Fixed
* All file/sequence/timeseries references to be nullable
* Filters on PlantInformation
* Filters on Watercourse
* Minor fixes to align resync with DMS changes

## [0.94.5] - 2024-05-31
### Changed
* Updated the SHOPRun class to provide a manual run flag; this is used in CogSHOP to skip output timeseries

## [0.94.4] - 2024-05-28

### Changed
* FDM v1 resync: Supporting default values in data_model_configuration also without specifying source
### Fixed
* FDM v1 resync:
  * Removing hardcoded reference to files/model.yaml, using all source_files in data_model_configuration instead
  * Handling empty files
  * Handling "missing" penstock_loss_factors and "missing" subtype lists

## [0.94.3] - 2024-05-11
### Added
* Support for FDM v1 resync

## [0.94.2] - 2024-05-06
### Fixed
* Bump pygen and regenerate powerops sdk to include bugfix for handling CDF native resource types; Sequence, File, in data class fields

## [0.94.1] - 2024-04-22
### Changed
* Updated all relations to sequences and time series to be nullable

## [0.94.0] - 2024-04-16
### Changed
* Updated BidDocument to provide needed context to frontend data model
* Standardized naming
  * SHOP -> Shop or shop
  * Timeseries -> TimeSeries

## [0.93.0] - 2024-04-09
### Changed
* Added ShopFile type as wrapper for shop files to be used in a Case in CogShop. Changes name from property shopScenarios to
scenarioSet in `ShopBasedPartialBidConfiguration` type.

## [0.92.0] - 2024-04-08
### Changed
* `v1` data model updates for enablement of multi method bid calculation

## [0.91.3] - 2024-03-21
### Added
* Regenerated the SDK with the latest version of `pygen` to get GraphQL feature for querying and parsing responses from cdf

## [0.91.2] - 2024-03-15
### Changed
* Changed property in `MultiScenarioMatrixRaw` for `shopResults` to `SHOPResultPriceProd` and added the `MultiScenarioMatrixRaw` to the `TotalBidCalculation` data model.

## [0.91.1] - 2024-03-13
### Added
* Added `bidDate` as a property to the `ShopPartialBidCalculationInput` view of the `SHOPBasedDayAheadBidProcess` data model.

# [0.91.0] - 2024-03-07
### Changed
* `MarketConfiguration` container updated
  - Changed the types of the property `priceSteps` to int, and `tickSize` to float
  - Removed property `marketType`
  - Added more descriptive descriptions to most properties
  - Removed property `marketType` and property descriptions from view based on this container

## [0.90.0] - 2024-03-12
### Changed
* `v1` data model with domain informed changes
  - Added a separate cogshop data model
  - Changes to `Scenario` type to accommodate for `Incremental mappings`. Preprocessor will now take in a `Scenario` and
    output a `Case`
  - Added wrapper for SHOP output timeseries - SHOPTimeSeries
  - Added interface for a generic SHOP result - SHOPResult

## [0.88.9] - 2024-03-07
### Fixed
* Transformation `AddWaterInTransit` apply returns input time series if discharge is empty

## [0.88.8] - 2024-03-06
### Changed
* Changed the types of the property `price_steps` to float, `tick_size` to float, and `trade_lot` to int
* in the `MarketConfiguration` view of the `SHOPBasedDayAheadBidProcess` data model.

## [0.88.7] - 2024-03-04
### Added
* Added the property `Mapping` to the `DayAheadConfiguration` data model.

## [0.88.6] - 2024-02-27
### Added
* Added the property `MarketConfiguration` to the `ShopPartialBidCalculationInput` view.

## [0.88.5] - 2024-02-27
### Changed
* Instantiating PowerOpsClient.from_client now reads toml settings file and supports overrides with kwargs.

## [0.88.4] - 2024-02-26
### Fixed
* Fixed data type for `shop_start` and `shop_end` fields on `Scenario` type.

## [0.88.3] - 2024-02-23
### Fixed
* Pygen regen sdk for `PlantShop`

## [0.88.2] - 2024-02-23
### Fixed
* Fixed DMS specification for `PlantShop` and `WatercoureShop` types.

## [0.88.1] - 2024-02-22
### Fixed
* Restored support for Python up to 3.12.*

## [0.88.0] - 2024-02-21
### Changed
* Changed PriceScenarios container to enable view to be more flexible / fit better with frontend
* Renamed *.nodes.yaml files to *.powerops-nodes.yaml and accommodate for this in resync init
* Regenerated pygen sdk with new datamodels v1
* Changed vies for Scenario and SHOPResult to use Mappings view instead of PriceScenario view
* Upgraded to cdf-tk v.b9

## [0.87.0] - 2024-02-19
### Added
* SDK call to fetch the pre-run file for a SHOP run

## [0.86.1] - 2024-02-18
### Added
* Several fixes to the new datamodels v1
* First draft of benchmarking model

## [0.86.0] - 2024-02-14
### Added
* Several fixes to the new datamodels v1
* Regenerated sdk with pygen for new datamodels

## [0.85.0] - 2024-02-11
### Added
* List of SHOP versions in CDF

## [0.84.0] - 2024-02-11
### Added
* Setup v1 Data Models generated with `pygen`

## [0.83.0] - 2024-02-08
### Changed
* Introduced `cognite-toolkit` in `powerops init` command.

## [0.82.5] - 2024-01-31
### Fixed
* Filter for `MultiScenarioMatrix` view

## [0.82.4] - 2024-01-31
### Fixed
* Filter for `BidMethod` view

## [0.82.3] - 2024-01-31
### Added
* `SHOPPriceScenarioResult` and `SHOPPriceScenario` to the `DayAheadBid` frontend model.

### Fixed
* Bug in Data Models with incorrect filters on all views. This is now fixed.

## [0.82.2] - 2024-01-29
### Fixed
* Bug in the HeightToVolume function not using the correct heights in the calculation

## [0.82.1] - 2024-01-25
### Fixed
* Bug in the filter for the views `CustomBidMethond` and `CustomBidMatrix` in the `DayAheadBid` frontend model.

## [0.82.0] - 2024-01-22
### Changed
* Frontend Models. Introduced `CustomBidMethond` and `CustomBidMatrix` in the `DayAheadBid` frontend model.

## [0.81.1] - 2024-01-23
### Fixed
* Fixed AddWaterInTransit function definition

## [0.81.0] - 2024-01-20
### Changed
* Removed Production Model from the SDK.
* Removed `powerops migrate` command.

## [0.80.4] - 2024-01-17
### Fixed
* Bugfix with using camelcase that caused files creation to fail

## [0.80.3] - 2024-01-11
### Fixed
* Script that generates new time series mapping config with the new transformations
* Changes to the script so that the translation functions can be called from the customer repos

## [0.80.2] - 2024-01-02
### Changed
* Parse timestamp to datetime object in a time zone naive fashion to stay consistent with preprocessor

## [0.80.1] - 2024-01-02
### Added
* FDM transformationsV2 instances to transformationsV2 pydantic translator
* Changed type of inputs for shop start and end times from `datetime` to `int`

## [0.80.0] - 2024-01-02
### Added
* Script for translating old transformation files to new ones
* Writing new transformation instances to CogShop model in addition to old ones, resulting in a duplication
of transformations nodes, but new transformation instances uses _Tr2_ prefix

## [0.79.0] - 2023-12-22
### Changed
* Single `PriceArea` type in data models.
* Support `dev` argument in `resync init` to destroy recreate views and
  data models in the development process.
* Regenerate clients with `pygen` `v0.33.0`
* Introduce `PowerOpsContainerModel` for an easy way to see all available
  containers in `PowerOps`

## [0.78.0] - 2023-12-22
### Changed
* Changes to views in `DayAheadBid` and `AFRRBid` models.
* Regenerate clients with `pygen` `v0.32.5`

## [0.77.0] - 2023-12-11
### Fixed
* Factory methods `from_client` and `from_toml` to PowerOps Client
* Regenerate clients with `pygen` `v0.32.1`

## [0.76.0] - 2023-12-11
### Fixed
* `AFRRBid` data model is now in `afrr_bid` folder (was `affr_bid`)

## [0.75.2] - 2023-12-11
### Changed
* A few renamings changes to the `DayAheadBid` model.

## [0.75.1] - 2023-12-11
### Changed
* Some minor changes to the `DayAheadBid` model.

## [0.75.0] - 2023-12-10
### Added
Three new data models added.
* `PowerOpsAsset` is an asset model that will replace the old `ProductionModel`.
* `DayAheadBid` which describes the day ahead bids, which is used in the PowerOps frontend to display the bids.
* `AFRRBid` which describes the aFRR bids, which is used in the PowerOps frontend to display the bids.

## [0.74.2] - 2023-12-04
### Added
* Optional kwargs for `PoweropsClient.form_settings` to override the defaults.

## [0.74.1] - 2023-12-04
### Added
* Optional input to DayaheadTrigger to rename plants in case they are modelled with different name in SHOP than
their asset names in CDF

## [0.74.0] - 2023-11-28
### Changed
* Upgraded cognite-sdk requirement to 7.x

## [0.73.5] - 2023-11-22
### Added
* Workflow events and relationships necessary to trigger a Dayahead workflow

## [0.73.4] - 2023-11-22
### Added
* New release workflow.zs

## [0.73.3] - 2023-11-20
### Added
* Noebook with basic usage of DayaheadTrigger
* Ability to fetch metadata info from prerun files that needs to go into SHOP run events in case the user does not
provide this with the instantiation of a DayaheadTrigger

## [0.73.2] - 2023-11-15
### Changed
* Changes to the shop api for triggering a shop run with a single casefile, and triggering a set of prerunfiles related
to a Case.
* Updates to DayaheadTrigger and DayaheadTriggerAPI classes to accommodate this

## [0.73.1] - 2023-11-07
### Changed
* Allow `v7` of `cognite-sdk`.

## [0.73.0] - 2023-10-25
### Changed
* Triggering a SHOP run from local environment is now easier, using `SHOPCase`.

## [0.72.0] - 2023-10-29
### Added
* Migration option for `resync` with support for the `Production` model
### Fixed
* Several inconsistencies in the `Production` v2 compared to v1 model

## [0.71.0] - 2023-10-28
### Added
* SDK for `CapacityBid` Model
### Fixed
* Bug in the DMS of `CapacityBid` Model

## [0.70.3] - 2023-10-25
### Improved
* Tweaked the logic for trimming of error messages for extraction pipeline runs.

## [0.70.2] - 2023-10-24
### Improved
* API call to create execution pipeline run will now be retried 5 times.

## [0.70.1] - 2023-10-17
### Fixed
* Return default value for `loss_factor` when the field is missing in model_raw file for a plant
* Add default value for a plant's `connection_losses`
* Safe return when no inlet reservoir is found for a plant

## [0.70.0] - 2023-10-16
### Added
* Extended the plant production model to include losses from water running between plant and inlet reservoir

## [0.69.0] - 2023-10-12
### Added
* `capacityBid` Model

## [0.68.1] - 2023-10-10
### Added
* `resync` support for benchmarking relationships to bid configurations

## [0.68.0] - 2023-10-10
### Added
* `validate` CLI command
### Changed
* Logs now write to stderr when running CLI commands

## [0.67.1] - 2023-10-06
### Added
* Documentation of transformations module

## [0.67.0] - 2023-10-03
### Added
* `TO_INT` transformation

## [0.66.0] - 2023-10-03
### Added
* `resync` support for writng back the processed shop model_raw file to local disk based on configuration parameter
in watercourse configuration

## [0.65.0] - 2023-10-02
### Added
* `resync` support having scenarios and model templates set outside itself in the `CogSHOP` model. This is achieved by
  `resync` filtering on the `source` field in the `Scenario` and `ModelTemplate` models.

## [0.64.0] - 2023-10-02
### Added
* Validation to shop related files. Shop files that need to be loaded in a specific order, needs to be accompanied by a
cog shop config. Validation will require this of the user.

## [0.63.0] - 2023-10-02
### Added
* Field `source` to `Scenario` and `ModelTemplate` in `CogSHOP` model.

## [0.62.2] - 2023-10-02
### Changed
* `from_type` and `to_type` as optional fields in Connections class to match file format in model_raw source files

## [0.62.1] - 2023-09-30
### Fixed
* Remove defaults on `resync` `Production` model. This caused changes to be hidden from `recync` and thus
  the CDF Assest not been updated.

## [0.62.0] - 2023-09-29
### Change
* Validation of generators by `resync`, made `p_min` optional. This is because it is only required by the water
  based methods.
### Fixed
* `resync` used wrong `command` file when updating the command node in the `CogSHOP` model.
* Standardize naming for scenarios uploaded in the `CogSHOP` model.

## [0.61.0] - 2023-09-29
### Added
* Validation of generators by `resync`, they now require `p_min`, `startcost`, and `penstock`.

## [0.60.2] - 2023-09-28
### Fixed
* `powerops plan/apply` did not handle the config for `NordPool` and `RKOM` correctly, leading these not to be updated. This is now fixed.

## [0.60.1] - 2023-09-27
### Fixed
* `powerops plan` showed differences for models when there were none due to faulty download of CDF model. This is now fixed.

## [0.60.0] - 2023-09-27
### Added
* Transformations module that holds all time series transformations.
* Two types of transformation classes:
  * **Transformation**: static - can be fully configured from a static configuration file
  * **DynamicTransformation**: dynamic - can be partly configured dynamically at runtime by rynning `pre_apply` function with necessary input parameters, before running `apply` on time series data

## [0.59.0] - 2023-09-26
### Added
* Support for loading `ProductionDM` from CDF.

# Changed
* `cognite.powerops.client._generated` has been regenerated with `pygen` `0.20.5`.

### Fixed
* Handle circular dependencies in data model when checking diffs.

## [0.58.4] - 2023-09-25
### Fixed
* External id format in `CogSHOP.Scenario` `view`, this is set to match the format used in functions.

## [0.58.3] - 2023-09-22
### Fixed
* Last fix had bugs. These new bugs are now now fixed.

## [0.58.2] - 2023-09-22
### Fixed
* The extraction pipeline run, `utils.cdf.extraction_pipelines.PipelineRun`, could produce messages above API limit
  with nested data structures. This is now fixed.

## [0.58.1] - 2023-09-22
### Fixed
* On Windows machines, calling `power.shop.trigger_case()` could cause the `case` file not bo be uploaded to CDF correctly
  due to a specific Windows encoding issue. This is now fixed.

## [0.58.0] - 2023-09-22
### Added
* Extended options for `SHOPRun.list()`.
* Added `SHOPRun.retrieve_latest()`

## [0.57.0] - 2023-09-21
### Added
* A model for the `aFRR` market bids. Available under `v2` models.

## [0.56.0] - 2023-09-21
### Added
* Support for deploying `v2` of `resync` models.

## [0.55.5] - 2023-09-19
### Fixed
* `ShopRun.get_log_files()` such that it handles non `utf-8` output from `SHOP`.

## [0.55.4] - 2023-09-19
### Fixed
* Fix JSON serialization in `utils.cdf.extraction_pipelines.PipelineRun`.

## [0.55.3] - 2023-09-19
### Fixed
* `powerops destroy` failed for `MarketModel` due to
* Some minor issues when running `powerops plan/apply` on an empty CDF project with only `powerops init
* Chunked writing of nodes and edges to maximum 1000 at a time.

## [0.55.2] - 2023-09-18
### Added
* Validation of price scenarios to ensure no duplicated price scenarios

## [0.55.1] - 2023-09-18
### Added
* Property `valid_shop_objects` to `WatercourseConfig` in `resync`. This is useful when creating time series
  mapping scripts.

## [0.55.0] - 2023-09-15
### Added
* `resync` now validates the TimeSeries Mappings against the shop model file.
* Extended `resync validate` to also run the transformations.

## [0.54.1] - 2023-09-14
### Fixed
* Running `powerops plan --as-extraction-pipeline-run` triggered change on not changed models.

## [0.54.0] - 2023-09-14
### Added
* Creation of `RKOM` scenarios in `resync`.

### Fixed
* Difference for `labels` when running `powerops plan` or `powerops apply`.
* Difference for `parent_assets` when running `powerops plan` or `powerops apply`.

## [0.53.3] - 2023-09-14
### Improved
* Better logging when running `powerops plan` with `--as-extraction-pipeline-run` option.

## [0.53.2] - 2023-09-13
### Fixed
* `ExtractionPipeline` with `truncation_keys` specified fails with `TypeError`. This is now fixed.

## [0.53.1] - 2023-09-13
### Fixed
* `PowerOpsClient.power.shop.trigger_case()` raise a `CogniteAPIError: Requesting principal has no user identifier`.
  This is now fixed.

### Changed
* `user_id` is replaced by `source` in the `ShopRun` model.

## [0.53.0] - 2023-09-13
### Added
* Support for destroying `MarketModel` and `ProductionModel`.

### Fixed
* Creation of parent assets and labels wthen running `powerops init`
* Overwriting initial status on each update `extraction pipeline` in `cognite.powerops.utils.cdf.extraction_pipeline`

## [0.52.0] - 2023-09-10
### Changes
* `PowerOpsClient.shop` API. Rewritten to be `SHOPRun` centric

## [0.51.0] - 2023-09-09
### Added
* `dry-run` option for `extraction pipeline` in `cognite.powerops.utils.cdf.extraction_pipeline.ExtractionPipelineCreate`

## [0.50.0] - 2023-09-09
### Improved
* Extraction Pipeline dump for `powerops plan --as-extraction-pipeline-run`

### Added
* Options for skipping keys to include in PipelineRun in `cognite.powerops.utils.cdf.extraction_pipeline.ExtractionPipelineCreate`

## [0.49.0] - 2023-09-09
### Changed
* Default verbose from `True` to `False` for all CLI commands

### Fixed
* Bug when running any `CLI` command with `--verbose` option. This is now fixed.

## [0.48.0] - 2023-09-09
### Changed
* Markdown output of `plan` and `apply` is now standardized.

### Added
* `verbose` option to `plan` command.
* `powerops init` command and resync method.
* `powerops validate` command and resync method.
* `powerops destroy` command and resync method.
* `dataset` API to `PowerOpsClient`.

### Removed
* `get_powerops_client` this has been replaced by `PowerOpsClient.from_settings`.

## [0.47.2] - 2023-09-08
### Fixed
* Fixed bug where comparison of two equal models resulted in change detected. This cause
  the `powerops plan --as-extraction-pipeline-run` to report an incorrect failed run.

## [0.47.1] - 2023-09-08
### Removed
* Removed some unused utils functions.

## [0.47.0] - 2023-09-08
### Changed
* Structure of repo.

## [0.46.1] - 2023-09-07
### Fixed
* Generated clients were regenerated to be compatible with `python-sdk` `6.20`.


## [0.46.0] - 2023-09-07
### Added
* `Events` connected to shop run will set its start and time to that of the shop run.
   If manually triggered, the source of the event will say "manual".

## [0.45.0] - 2023-09-07
### Added
* Markdown output option for `powerops apply`.

### Fixed
* `resync apply` fails to apply changes if there were either `added` or `changed`. This is now fixed.

## [0.44.1] - 2023-09-05
### Fixed
* `resync apply` fails with if there are changes in the `CogShop1Asset` models due to nodes and edges have to be
  added with nodes first, while when removing edges must be first. This is now fixed.

## [0.44.0] - 2023-09-05
### Added
* `resync` now generates Scenarios for `CogShop1Asset` models.

## [0.43.5] - 2023-09-04
### Fixed
* Deletion of assets raised a `ValueError`. This is now fixed.

## [0.43.4] - 2023-09-04
### Fixed
* Missing comma in `json` dumped in `CogShop1Asset.transformations` `arguments` argument.

## [0.43.3] - 2023-09-03
### Fixed
* Third party logger not propagate when running CLI.

## [0.43.2] - 2023-09-03
### Fixed
* Log level of `requests-oauthlib` set to `WARNING` when running CLI.

## [0.43.1] - 2023-09-03
### Fixed
* Key used for error logging when running the CLI command `powerops plan` with option `as_extraction_pipeline_run`.

## [0.43.0] - 2023-09-03
### Added
* The CLI command `powerops plan` with option `as_extraction_pipeline_run`.

## [0.42.3] - 2023-09-01
### Fixed
* The CLI commands `powerops plan` and `powerops apply` used a custom logger configuration instead of default.

## [0.42.2] - 2023-09-01
### Fixed
* The CLI commands `powerops plan` and `powerops apply` failed with `ValueError` if there were new sequences added
  and while there were still some unchanged. This is now fixed.

## [0.42.1] - 2023-09-01
### Fixed
* The CLI commands `powerops plan` and `powerops apply` failed with `CogniteAPIError` if there were new
  sequences added. This is now fixed.


## [0.42.0] - 2023-08-31
### Added
* Moved `powerops plan` support Markdown output..


## [0.41.0] - 2023-08-30
### Added
* Moved `power_client.shop.runs.list()` method.


## [0.40.1] - 2023-08-30
### Fixed
* `powerops apply` only run `add`, `remove` or `changed` if there are any changes to the models. This is now fixed such
   that `add`, `remove` or `changed` can all run.

## [0.40.0] - 2023-08-30
### Changed
* Removed use of hashing for external ids in `CogShop1Asset` asset model.


## [0.39.1] - 2023-08-29
### Fixed
* Fixed bug when running `powerops plan` or `powerops apply` in a Python `3.11` environment.
  This raised `NotImplementedError`. This is now fixed.

## [0.39.0] - 2023-08-29
### Added
* The `powerops apply` now deletes resource in CDF if they are not present in the configurations.
* Support for detection changes in the content of `Sequences` and `Files` and update them in CDF if they have changed.

### Improved
* Display of differences when running `powerops plan` or `powerops apply` is now more readable.
* Significant performance improvements when running `powerops plan` and `powerops apply`. Example,
  running `powerops plan` for the `ProductionModel` went from 77 seconds to 8 seconds for real use case.



## [0.38.3] - 2023-08-25
### Fixed
* Fixed bugs when running `powerops plan` some edge case could raise and error as a missing argument
  to a function.


## [0.38.2] - 2023-08-25
### Fixed
* Fixed bugs when running `powerops plan` without `--dump-folder`. This raised `NotImplementedError`. Now
  difference between the models in CDF and the models generated by `resync` is printed to the terminal.


## [0.38.1] - 2023-08-25
### Fixed
* Fixed bugs when running `powerops plan`:
  * Handle nested data structures.
  * Only read from the correct data set.


## [0.38.0] - 2023-08-24
### Fixed
* Fixed bugs when running `powerops plan`.

### Changed
* Running `powerops plan` or `powerops apply` will now run the default models `CogShop1Asset`, `ProductionModel`, and
  `MarketModel` if no models are specified in the CLI call.

## [0.37.0] - 2023-08-23
### Added
* Added generator availability time series to the generator class

## [0.36.0] - 2023-08-18
### Added
* Extended the `powerops plan` options. Now you can dump the entire models to file with the argument `--dump-folder`.
* Summary difference between the models in CDF and the models generated by `resync` is now printed to the terminal.

## [0.35.0] - 2023-08-11
### Added
* Extended the `Production` data model with a relation from `Plant`to `Watercourse`.

## [0.34.1] - 2023-08-11
### Fixed
* Mapping of models names accepted by the CLI and the models names' in `resync`.


## [0.34.0] - 2023-08-03
### Added
* Ability to retrieve production data model from CDF with relationships to other resources.
* Ability to compare production data model in CDF with production data models generated by `resync`.

## [0.33.1] - 2023-08-03

### Changed
* ResyncConfig.cogshop to have time_series_mappings as optional.

## [0.33.0] - 2023-07-26

### Removed
* The `prepocessor` package.


## [0.32.0] - 2023-07-25

### Changed
* Data Model for dayahead model to match better the asset version of the data models.


## [0.31.0] - 2023-07-24

### Changed
* Data Model for market, production, and cogshop to match better the asset version of the data models.


## [0.30.0] - 2023-07-24

### Added
* Ability to retrieve production data model from CDF assets. Relationships to other resources not yet supported.

## [0.29.1] - 2023-07-21

### Added

* Validation that all timeseries exists in CDF before creating relationships to them.


## [0.29.0] - 2023-07-20

### Changed

* Removed all interfaces from data models.


## [0.28.1] - 2023-07-18

### Fixed

* The command `powerops plan` failed with `CogniteAPIError`. This is now fixed.


## [0.28.0] - 2023-07-16

### Added

* Created DataModels for Market, now split into `DayAhead`, `RKOM`, and `Benchmark`.
* Split the CogShop model into a `CogShopAsset` and a `CogShopDataModel`.
* Option for running `deploy`, `apply` and `plan` on a subset of data models in the CLI.
* Added a summary view of each data model and an option for user input before deploying.


### Changed

* Upgraded to `0.12.0` of `pygen` which generates one SDK for multiple data models.
* Changed the production and cogshop data models to better match the configuration files.

### Improved

* Split approval test into one test per model.

## [0.27.1] - 2023-07-13

### Fixed

* Bug when deploying the market model, missing type `ValueTransformation`.


## [0.27.0] - 2023-07-13

### Added

CLI commands

* `deploy` for deploying the PowerOps models to CDF.
* `show` for displaying the PowerOps models in the terminal.
* `--version` for displaying the version of the PowerOps-SDK package.

### Changed

* `production` to reflect the content from the `resync` package.
* `market` to reflect the content from the `resync` package.
* `cogshop` to reflect the content from the `resync` package.

## [0.26.0] - 2023-07-11

### Changed

* The `bootstrap` package has been completely rewritten and renamed `resync`. The functionality is the same, with no
  changes to the CLI calls `apply` and `plan`. However, all the internal code has been rewritten to be more robust and
  easier to maintain.
* `BREAKING_CHANGE:` The new `resync` requires the configuration files to be structured into `production`, `market`, and
  `cogshop`. The motivation for this change is to make it easier to navigate the `PowerOps` model by grouping into
  smaller submodels, and make the dependencies between the models clear, `Market` depends on `Production.price_areas`
  and `CogShop` depends on `Production.watercourses`.

## [0.25.3] - 2023-07-11

### Fixed

* Preprocessor graphQL query missed optional fields.


## [0.25.2] - 2023-07-07
### Fixed

* Bug causing the md5 hash used in the `powerops plan` command to be OS dependent. This is now fixed.



## [0.25.1] - 2023-07-06
### Changed

* Upgrade to `pydantic` 2.0.


## [0.25.0] - 2023-07-05
### Changed

* Refactored package into `bootstrap`, `clients` and `preprocessor` modules. This is likely a breaking change for most users of the package.


## [0.24.1] - 2023-06-30
### Removed

* Removed unused settings.


## [0.24.0] - 2023-07-03
### Added

* Data Models for bootstrap resources


## [0.23.0] - 2023-06-29
### Added

* Re-implemented populating data to DM.


## [0.22.2] - 2023-06-29
### Fixed

* Fix for getting PowerOpsClient instance without settings file.

### Changed

* Instantiating PowerOpsClient with config form settings files now has to be done via a separate factory method.


## [0.22.1] - 2023-06-29
### Fixed

* Fixed the data model field names in the preprocessor package.


## [0.22.0] - 2023-06-29
### Removed

* Dependency on cognite-gql-pygen
* Temporary removed DM code which needs to be rewritten for new DM client.

### Added

* Settings validation.


## [0.21.0] - 2023-06-28
### Changed

* Changed how preprocessor fetches shop files from CDF
* Preprocessor will fetch a file from CDF that says explicitly what files to fetch and in what order they should be
loaded to shop prior to a run
* This will support the option for having several cutfiles and module series files per watercourse
* Breaking change for preprocessor as it will fail if the CogShop file config does not exist in CDF

## [0.20.0] - 2023-06-25
### Added

* Added file type for shop files of type "module_series"
* Added option to provide additional string in file names and external ID for files uploaded to CDF

## [0.19.1] - 2023-06-26
### Fixed

* Adding "generator_" prefix to the external_id of generators

## [0.19.0] - 2023-06-26
### Added

* Add p-min to cdf generator object

## [0.18.0] - 2023-06-23
### Added

* Added ability to compare the yamls of shop runs and view it in a notebook.
* Example Notebook for showing results and comparing shop runs.


## [0.17.0] - 2023-06-21
### Added

* Support for multiple cut files on the preprocessor subpackage.


## [0.16.0] - 2023-06-21
### Added

* Add metadata field "no_shop" to bid process config.


## [0.15.2] - 2023-06-21
### Added

* Documentation metadata to package, and a minimum introduction to the package in the documentation.


## [0.15.1] - 2023-06-19
### Changed

* Upd required versions for cognite-sdk and cognite-gql-pygen.


## [0.15.0] - 2023-06-19
### Added

* Ability to find time series keys in the shop generated YAML files, including some filtering.
* Ability to plot several time series from one shop generated YAML file using matplotlib.
* Ability to compare a time series from different shop runs in a plot view if they are accessible by the same keys.


## [0.14.0] - 2023-06-19
### Added

* CogShop preprocessor, moved from dedicated repo.

## [0.13.0] - 2023-06-19
### Changed

* Removed unnecessary dependencies


## [0.12.0] - 2023-06-08
### Added

* Ability to trigger SHOP runs via CogShop.
* Retrieval of ShopRun results.

## [0.11.0] - 2023-06-08
### Added

* Allow customers to specify default bid config per price area

## [0.10.0] - 2023-06-05
### Added

* Added validation to rkom_bid_combination config for reference to rkom_bid_process external IDs.

## [0.9.2] - 2023-05-30
### Fixed

* Fixed bug for overriding CDF parameters from env variables

## [0.9.1] - 2023-05-26
### Fixed

* Typo in warning message for plant display name
* Removed redundant warning message for reservoir display name and order

## [0.9.0] - 2023-05-25
### Changed

* Changed location of all config related class definitions located in dataclasses module
to config.py module
* Deleted unused modules

## [0.8.0] - 2023-05-23
### Added

* Basic handling for YAML case files.
* pytest now runs doctests.

### Improved

* Shorter imports for commonly used files

### Changed

* Renamed `powerops.core` attribute to `powerops.cdf`

## [0.7.1] - 2023-05-23
### Fixed

* Fixed bug with external id being stored as int in bootstrap resources

## [0.7.0] - 2023-05-22
### Added

* Added test for the water value based bid generation (WVBBG) time series contextualization
* Changed the mapping format for wvbbg from csv to yaml.
* Added time series contextualization for generators as well

## [0.6.0] - 2023-05-22
### Added

* Added support for reading configuration parameters for Cognite Client from env variables in addition to yaml file.

## [0.5.0] - 2023-05-16
### Added

* Support adding direct head values to water value based bid generation (WVBBG) time series contextualization.

## [0.4.2] - 2023-05-15
### Fixed

* Shop files external ID suffix to get correct preview print with bootstrap "plan"


## [0.4.1] - 2023-05-15
### Fixed

* Import errors in `PowerOpsClient`


## [0.4.0] - 2023-05-12
### Added

* Scaffolding for a `SHOPApi` implementation.


## [0.3.1] - 2023-05-12
### Fixed

* Fixed missing `README.md` in package metadata.


## [0.3.0] - 2023-05-12
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
