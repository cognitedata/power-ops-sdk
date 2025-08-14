# Setting up a New PowerOps Project

## Initial CDF Setup

Follow steps to create and set up a CDF project, with the required access groups and datasets (recommended to use toolkit for project setup steps). See the [power_ops_template](toolkit/modules/power_ops_template/) folder for examples of some simple access groups to get started and the PowerOps required data sets.

Once you have admin credentials to the project proceed to the below steps.

## PowerOps Toolkit

- Provide PowerOps/project team with a set of client credentials that have enough privileges to run toolkit. These will be added as secrets to a Github environment (see [this script](scripts/update_gh_env_secrets.py)) in this repo for the CI/CD pipeline. The project will then be added in the [pipeline script](.github/workflows/toolkit-release.yml).
  - Note that the GitHub Environment name must match the CDF project name exactly, and that there are two places in the pipeline script where the project needs to be added.

- Create a PR to configure a new toolkit config in this repo to deploy the PowerOps data model to the new project. See the [staging configuration](toolkit/config.staging.yaml) as an example.
  - No need to run toolkit locally as the release process will automatically deploy the resources.

**Note:** The CI/CD pipeline in this repo is *only* responsible for deploying the PowerOps data model (ie. resources in the power_model module) so any other resources should be deployed using toolkit configured separately in a customer repo. Refer to the `power_ops_template` and `resync` modules as examples to be used in the customer repo.

## CogShop & PowerOps UI

- Provide PowerOps team a set of credentials with the follow information and access to read/write to the PowerOps data model:
  - CDF cluster (ie. `az-power-no-northeurope`)
  - CDF project name (ie. `power-ops-staging`)
  - Tenant ID
  - Client ID
  - Client Secret

**Note:** only the CogShop configuration uses the credentials

## Functions & Workflows

If the PowerOps functions are to be used, provide the PowerOps team with credentials so they can configure the PowerOps functions repo to automatically deploy the functions and their updates.

To use workflows with these functions see the example toolkit configuration in the [power_ops_template/workflows](toolkit/power_ops_template/workflows) module.
