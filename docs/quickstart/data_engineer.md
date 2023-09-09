What we mean with a data engineer is a person with the following type of responsibilities:

* Setup Cognite Data Fusion capabilities for the power ops opertaions.
* Populate Cognite Data Fusion with the configuration that is used to run power ops.

The main tool from this SDK to help the data engineer to accomplish these tasks is the `powerops` CLI tool,
or `resync` if you are using the Python code directly.

## Organization of the configuration

The configuration files are expected to be checked into a Git repository, and have the following structure:

```text
    📦my_config_dir
     ┣ 📂cogshop - The CogSHOP configuration files
     ┣ 📂market - The Market configuration for DayAhead, RKOM, and benchmarking.
     ┣ 📂production - The physical assets configuration, Watercourse, PriceArea, Genertor, Plant  (SHOP centered)
     ┗ 📜settings.yaml - Settings for resync.
```

## Setup Cognite Data Fusion Data Models

To setup the data models in Cognite Data Fusion, you need to run the following command:

```bash
powerops init
```

This will deploy the necessary data models to Cognite Data Fusion.

## Populate Cognite Data Fusion with configuration

You can first validate the configuration with the following command:

```
powerops validate [MyConfigDir] [MarketName]
```
**Note** the validation is done when you run `plan` or `apply` as well.

Then, in case Cognite Data Fusion is empty, you can populate it with the following command:
```
powerops apply [MyConfigDir] [MarketName]
```

## Update Configuration in Cognite Data Fusion

Do the necessary changes to the configuration files, and then run the following command:

```
powerops plan [MyConfigDir] [MarketName] --format markdown
```

This will show you the changes that will be applied to Cognite Data Fusion.

If you are happy with the changes, you can apply them with the following command:

```
powerops apply [MyConfigDir] [MarketName]
```

## Removing all Configuration from Cognite Data Fusion

If you want to remove all configuration from Cognite Data Fusion, you can run the following command:

```
powerops destroy
```
